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
from kafka import KafkaClient
from kafka import SimpleConsumer
import logging

from monasca_events_engine.common.repositories import exceptions
from monasca_events_engine.common.repositories.mysql.streams_repository \
    import StreamsRepository
from monasca_events_engine import stream_helpers as sh
import monascastatsd

log = logging.getLogger(__name__)


class EventProcessorBase(object):
    """EventProcessorBase

    The base class for the EventProcessor and PipelineProcessor.
    """

    dimensions = {
        'service': 'monitoring', 'component': 'monasca-events-engine'}

    def __init__(self, conf):
        self.conf = conf
        self.streams_repo = StreamsRepository()

    def stream_defs_from_database(self):
        slist = list()
        try:
            stream_definition_rows = \
                self.streams_repo.get_all_stream_definitions()
            for row in stream_definition_rows:
                row['fire_criteria'] = json.loads(row['fire_criteria'])
                row['select_by'] = json.loads(row['select_by'])
                row['group_by'] = json.loads(row['group_by'])
                w_stream = sh.stream_def_to_winchester_format(
                    row)
                slist.append(w_stream)
        except exceptions.RepositoryException as e:
            log.error(e)

        return slist

    def stream_definition_consumer(self, conf, lock, group, manager):
        '''Stream Definition Consumer

        Stream definition consumer runs as a thread for the event processor and
        pipeline processor processes.

        :param CONF.cfg conf: the conf object. (clarity, CONF.cfg is global)
        :param thread.Lock() lock: Used to lock manager access.
        :param string group: The Kafka group for stream definitions.
        :param object manager: trigger_manager, or pipeline_manager object.
        '''
        kafka_url = conf.kafka.url
        group = conf.kafka.stream_def_group
        topic = conf.kafka.stream_def_topic
        fetch_size = conf.kafka.events_fetch_size_bytes
        buffer_size = conf.kafka.events_buffer_size
        max_buffer = conf.kafka.events_max_buffer_size
        kafka = KafkaClient(kafka_url)
        consumer = SimpleConsumer(
            kafka,
            group,
            topic,
            auto_commit=True,
            # auto_commit_every_n=None,
            # auto_commit_every_t=None,
            # iter_timeout=1,
            fetch_size_bytes=fetch_size,
            buffer_size=buffer_size,
            max_buffer_size=max_buffer)

        consumer.seek(0, 2)

        statsd = monascastatsd.Client(name='monasca',
                                      dimensions=self.dimensions)
        stream_definitions_created = \
            statsd.get_counter('stream_definitions_created')
        stream_definitions_deleted = \
            statsd.get_counter('stream_definitions_deleted')

        for s in consumer:
            offset, message = s
            stream_def = json.loads(message.value)

            if 'stream-definition-created' in stream_def:
                log.debug('Received a stream definition created event')
                stream_create = sh.stream_def_to_winchester_format(
                    stream_def['stream-definition-created'])
                slist = list()
                slist.append(stream_create)
                lock.acquire()
                try:
                    manager.add_trigger_definition(slist)
                    stream_definitions_created.increment()
                except Exception as e:
                    log.exception(e)
                lock.release()
            elif 'stream-definition-deleted' in stream_def:
                log.debug('Received a stream-definition-deleted event')
                name = sh.stream_unique_name(
                    stream_def['stream-definition-deleted'])
                lock.acquire()
                try:
                    manager.delete_trigger_definition(name)
                    stream_definitions_deleted.increment()
                except Exception as e:
                    log.exception(e)
                lock.release()
            else:
                log.error('Unknown event received on stream_def_topic')
