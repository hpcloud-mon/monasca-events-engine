# Copyright (c) 2014 Hewlett-Packard Development Company, L.P.
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
from logging.config import fileConfig
from winchester.config import ConfigManager
from winchester.pipeline_manager import PipelineManager
from event_processor import EventProcessor
import time

log = logging.getLogger(__name__)


class PipelineProcessor():
    """ PipelineProcessor uses the stacktach PipelineManager to load pipeline handlers, and 
        process ready and expired streams. The PipelineManager contains a TriggerManager so that
        handlers can add events to the TriggerManager, adding more events to the stream.
        The TriggerManager within the PipelineManager will need to be initialized with 
        Triggerdefs dynamically.
    """
    def __init__(self, winchester_config):      
        self.winchester_config = winchester_config
        self.config = ConfigManager.load_config_file(self.winchester_config)
        
    def run(self):
        if 'logging_config' in self.config:
            fileConfig(self.config['logging_config'])
        else:
            logging.basicConfig()
            if 'log_level' in self.config:
                level = self.config['log_level']
                level = getattr(logging, level.upper())
                logging.getLogger('winchester').setLevel(level)
        # start the PipelineManager
        self.pipe = PipelineManager(self.config)
    
        #  add trigger defs from the DB at startup
        self.pipe.add_trigger_definition(EventProcessor.trig_def_fc1_tenant406904_filter)
        self.main_loop()
        
    def main_loop(self):
        while True:
            
            # read from kafka stream-def-event topic and add any new trigger definitions
            self.pipe.add_trigger_definition(EventProcessor.trig_def_fc1_tenant123456_filter)
            self.pipe.add_trigger_definition(EventProcessor.trig_def_fc2_rackspace_billing)
            
            self.pipe.delete_trigger_definition('fc1_trigger_123456')
                       
            fire_ct = self.pipe.process_ready_streams(self.pipe.pipeline_worker_batch_size)
            expire_ct = self.pipe.process_ready_streams(self.pipe.pipeline_worker_batch_size,
                                                   expire=True)

            if (self.pipe.current_time() - self.pipe.last_status).seconds > self.pipe.statistics_period:
                self.pipe._log_statistics()

            if not fire_ct and not expire_ct:
                log.debug("No streams to fire or expire. Sleeping...")
                time.sleep(self.pipe.pipeline_worker_delay)

