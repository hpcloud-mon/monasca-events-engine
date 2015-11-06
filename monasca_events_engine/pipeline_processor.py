# (C) Copyright 2015 Hewlett Packard Enterprise Development Company LP
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

import logging
import threading
import time

from monasca_events_engine.event_processor_base import EventProcessorBase
import monascastatsd
from winchester.config import ConfigManager
from winchester.pipeline_manager import PipelineManager

log = logging.getLogger(__name__)


class PipelineProcessor(EventProcessorBase):

    """Pipeline Processor

        PipelineProcessor uses the stacktach PipelineManager to
        load pipeline handlers, and process ready and expired streams.
        The PipelineManager contains a TriggerManager so that
        handlers can optionally add more events to the TriggerManager
        filtered stream. The TriggerManager within the PipelineManager
        is initialized with stream definitions dynamically.
    """

    def __init__(self, conf):
        super(PipelineProcessor, self).__init__(conf)
        self._winchester_config = conf.winchester.winchester_config
        self._config_mgr = ConfigManager.load_config_file(
            self._winchester_config)
        self._group = conf.kafka.stream_def_pipe_group
        self._pm_lock = threading.Lock()
        self._pipe = PipelineManager(self._config_mgr)

    def run(self):

        # read stream-definitions from DB at startup and add
        stream_defs = self.stream_defs_from_database()
        if len(stream_defs) > 0:
            log.debug(
                'Loading {} stream definitions from the DB at startup'.format(
                    len(stream_defs)))
            self._pipe.add_trigger_definition(stream_defs)

        # start threads
        self.stream_def_thread = threading.Thread(
            name='stream_defs_pipe',
            target=self.stream_definition_consumer,
            args=(self.conf, self._pm_lock, self._group, self._pipe,))

        self.pipeline_ready_thread = threading.Thread(
            name='pipeline',
            target=self.pipeline_ready_processor,
            args=(self._pm_lock, self._pipe,))

        log.debug('Starting stream_defs_pipe and pipeline threads')
        self.stream_def_thread.start()
        self.pipeline_ready_thread.start()

        self.stream_def_thread.join()
        self.pipeline_ready_thread.join()
        log.debug('Exiting')

    def pipeline_ready_processor(self, lock, pipe):
        statsd = monascastatsd.Client(name='monasca',
                                      dimensions=self.dimensions)
        fired_streams = statsd.get_counter('fired_streams')
        expired_streams = statsd.get_counter('expired_streams')

        while True:

            lock.acquire()
            try:
                fire_ct = pipe.process_ready_streams(
                    pipe.pipeline_worker_batch_size)
                expire_ct = pipe.process_ready_streams(
                    pipe.pipeline_worker_batch_size,
                    expire=True)
                if fire_ct > 0:
                    fired_streams.increment()
                if expire_ct > 0:
                    expired_streams.increment()
            except Exception as e:
                log.exception(e)
            finally:
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
