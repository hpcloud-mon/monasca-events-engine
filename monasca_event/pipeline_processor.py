# Copyright (c) 2015 Hewlett-Packard Development Company, L.P.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
# implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
from kafka import KafkaClient, SimpleConsumer
import logging
from logging.config import fileConfig
import threading
import time

import event_processor
from winchester.config import ConfigManager
from winchester.pipeline_manager import PipelineManager


log = logging.getLogger(__name__)


def pipe_stream_definition_consumer(kafka_config, lock, pipe):
    kafka_url = kafka_config['url']
    group = kafka_config['stream_def_pipe_group']
    topic = kafka_config['stream_def_topic']
    kafka = KafkaClient(kafka_url)
    consumer = SimpleConsumer(kafka,
                              group,
                              topic,
                              auto_commit=True,
                              # auto_commit_every_n=None,
                              # auto_commit_every_t=None,
                              # iter_timeout=1,
                              fetch_size_bytes=kafka_config[
                                  'events_fetch_size_bytes'],
                              buffer_size=kafka_config['events_buffer_size'],
                              max_buffer_size=kafka_config['events_max_buffer_size'])

    consumer.seek(0, 2)

    for s in consumer:
        offset, message = s
        stream_def = json.loads(message.value)

        if 'stream-definition-created' in stream_def:
            log.debug('Received a stream-definition-created event')
            stream_create = event_processor.stream_def_to_winchester_format(
                stream_def['stream-definition-created'])
            lock.acquire()
            pipe.add_trigger_definition(stream_create)
            lock.release()
        elif 'stream-definition-deleted' in stream_def:
            log.debug('Received a stream-definition-deleted event')
            name = event_processor.stream_unique_name(
                stream_def['stream-definition-deleted'])
            lock.acquire()
            pipe.delete_trigger_definition(name)
            lock.release()
        else:
            log.error('Unknown event received on stream_def_topic')


class PipelineProcessor():

    """ Pipeline Processor

        PipelineProcessor uses the stacktach PipelineManager to load pipeline handlers, and
        process ready and expired streams. The PipelineManager contains a TriggerManager so that
        handlers can optionally add more events to the TriggerManager filtered stream.
        The TriggerManager within the PipelineManager will need to be initialized with
        stream definitions dynamically.
    """

    def __init__(self, kafka_config, winchester_config):
        self.winchester_config = winchester_config
        self.kafka_config = kafka_config
        self.config_mgr = ConfigManager.load_config_file(
            self.winchester_config)

    def run(self):
        if 'logging_config' in self.config_mgr:
            fileConfig(self.config_mgr['logging_config'])
        else:
            logging.basicConfig()
            if 'log_level' in self.config_mgr:
                level = self.config_mgr['log_level']
                level = getattr(logging, level.upper())
                logging.getLogger('winchester').setLevel(level)

        self.pm_lock = threading.Lock()
        self.pipe = PipelineManager(self.config_mgr)

        #  TODO add trigger defs from the DB at startup

        # start threads
        self.stream_def_thread = threading.Thread(name='stream_defs_pipe',
                                                  target=pipe_stream_definition_consumer,
                                                  args=(self.kafka_config, self.pm_lock, self.pipe,))

        self.pipeline_ready_thread = threading.Thread(name='pipeline',
                                                      target=self.pipeline_ready_processor,
                                                      args=(self.pm_lock, self.pipe,))

        self.stream_def_thread.start()
        self.pipeline_ready_thread.start()

        self.stream_def_thread.join()
        self.pipeline_ready_thread.join()
        log.debug('Exiting')

    def pipeline_ready_processor(self, lock, pipe):
        while True:

            lock.acquire()
            fire_ct = pipe.process_ready_streams(
                pipe.pipeline_worker_batch_size)
            expire_ct = pipe.process_ready_streams(pipe.pipeline_worker_batch_size,
                                                   expire=True)
            lock.release()

            if (pipe.current_time() -
                    pipe.last_status).seconds > pipe.statistics_period:
                pipe._log_statistics()

            if not fire_ct and not expire_ct:
                log.debug("No streams to fire or expire. Sleeping...")
                time.sleep(pipe.pipeline_worker_delay)
            else:
                log.debug(
                    "Fired {} streams, Expired {} streams".format(
                        fire_ct,
                        expire_ct))
