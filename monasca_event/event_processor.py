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

from kafka import KafkaClient, SimpleConsumer
import json
import logging
from winchester.config import ConfigManager
from winchester.trigger_manager import TriggerManager


log = logging.getLogger(__name__)


class EventProcessor():
    """ EventProcessor reads events from kafka, and adds them to the 
        stacktach winchester TriggerManager
    """
    def __init__(self, kafka_url, group, event_topic, winchester_config):      
        self.kafka_url = kafka_url
        self.group = group
        self.topic = event_topic
        self.winchester_config = winchester_config
        self.kafka = KafkaClient(self.kafka_url)
        self.consumer = SimpleConsumer(self.kafka,
                                       group,
                                       event_topic,
                                       auto_commit=True)
        self.config = ConfigManager.load_config_file(winchester_config)
        self.trigger_manager = TriggerManager(self.config)

    def consume_raw(self):
        for message in self.consumer:
            log.debug(message.message.value.decode('utf8'))
            decoded = json.loads(message.message.value)
            raw_event = decoded['event']['dimensions']
            # add_notification will distill the event before saving it.
            self.trigger_manager.add_notification(raw_event)

    def consume_transformed(self):
        # read events from kafka
        for message in self.consumer:
            log.debug(message.message.value.decode('utf8'))

            decoded = json.loads(message.message.value)
            log.debug(json.dumps(decoded, sort_keys=True, indent=4))
            
            event = decoded['event']
            log.debug('event: %s', event)

            # add them to winchester TriggerManager
            self.trigger_manager.add_event(event)        

    def run(self):
        self.consume_raw()
        
        
