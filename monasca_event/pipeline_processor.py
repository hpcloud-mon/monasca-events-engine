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

log = logging.getLogger(__name__)


class PipelineProcessor():
    """ PipelineProcessor uses the stacktach PipelineManager to load pipeline handlers, and 
        process ready and expired streams. 
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
        pipe = PipelineManager(self.config)
        pipe.run()
        
