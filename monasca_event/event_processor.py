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

import datetime
from kafka import KafkaClient, SimpleConsumer
import json
import simplejson
import logging
import time
import uuid

from winchester.config import ConfigManager
from winchester.trigger_manager import TriggerManager
from datetime import datetime

log = logging.getLogger(__name__)


class EventProcessor():

    """
    The EventProcessor reads distilled events from the kafka 'transformed-events' topic, and adds them to the
    stacktach winchester TriggerManager which contains the TriggerDefs (defs by tenant, and global defs for OPS).
    Adding distilled events to the TriggerManager adds all distilled events into the Mysql DB.
    The TriggerManager keeps temporary streams of events in Mysql - filtering by 'match criteria'
    and grouping by 'distinguished by'.  The streams are deleted when the fire criteria has been met. """

    """ test data """
    """ trigger defs for fire criteria 1 - looking for start and end"""
    trig_def_fc1 = [{'distinguished_by': ['instance_id'],
                     'fire_criteria': [{'event_type': 'compute.instance.create.start'},
                                       {'event_type': 'compute.instance.create.end'}],
                     'match_criteria': [{'event_type': 'compute.instance.create.*'}],
                     'name': 'fc1_trigger',
                     'debug_level': 2,
                     'expiration': '$last + 1h',
                     'fire_pipeline': 'test_pipeline',
                     'expire_pipeline': 'test_expire_pipeline'}]

    trig_def_fc1_tenant406904_filter = [{'distinguished_by': ['instance_id'],
                                         'fire_criteria': [{'event_type': 'compute.instance.create.start'},
                                                           {'event_type': 'compute.instance.create.end'}],
                                         'match_criteria': [{'traits': {'tenant_id': '406904'},
                                                             'event_type': 'compute.instance.create.*'}],
                                         'name': 'fc1_trigger_406904',
                                         'debug_level': 2,
                                         'expiration': '$last + 15s',
                                         'fire_pipeline': 'test_pipeline',
                                         'expire_pipeline': 'test_expire_pipeline'}]

    trig_def_fc1_tenant123456_filter = [{'distinguished_by': ['instance_id'],
                                         'fire_criteria': [{'event_type': 'compute.instance.create.start'},
                                                           {'event_type': 'compute.instance.create.end'}],
                                         'match_criteria': [{'traits': {'tenant_id': '123456'},
                                                             'event_type': 'compute.instance.create.*'}],
                                         'name': 'fc1_trigger_123456',
                                         'debug_level': 2,
                                         'expiration': '$last + 24h',
                                         'fire_pipeline': 'test_pipeline',
                                         'expire_pipeline': 'test_expire_pipeline'}]

    """ test adding events to cause fire criteria """
    distilled_events_fc1_tenant_406904 = [{'os_distro': 'com.ubuntu',
                                           'event_type': 'compute.instance.create.start',
                                           'service': 'publisher-302689',
                                           'instance_type': '512MB Standard Instance',
                                           'tenant_id': '406904',
                                           'instance_flavor_id': '2',
                                           'hostname': 'server-462185',
                                           'host': 'publisher-302689',
                                           'instance_flavor': '512MB Standard Instance',
                                           'instance_id': '123-3b0f-4057-b377-b65131e8532e',
                                           'os_version': '12.04',
                                           'state': 'building',
                                           'os_architecture': 'x64',
                                           'timestamp': datetime.utcnow(),
                                           'request_id': 'req-d096b6de-f451-4d00-bff0-646a8c8a23c3',
                                           'message_id': '19701f6c-f51f-4ecb-85fb-7db40277627d'},
                                          {'os_distro': 'com.ubuntu',
                                           'message_id': '2ae21707-70ae-48a2-89c0-b08b11dc0b1a',
                                           'service': 'publisher-302689',
                                           'instance_type': '512MB Standard Instance',
                                           'tenant_id': '406904',
                                           'instance_flavor_id': '2',
                                           'hostname': 'server-462185',
                                           'host': 'publisher-302689',
                                           'instance_flavor': '512MB Standard Instance',
                                           'instance_id': '123-3b0f-4057-b377-b65131e8532e',
                                           'os_version': '12.04',
                                           'state': 'active',
                                           'os_architecture': 'x64',
                                           'timestamp': datetime.utcnow(),
                                           'request_id': 'req-d096b6de-f451-4d00-bff0-646a8c8a23c3',
                                           'launched_at': datetime.utcnow(),
                                           'event_type': 'compute.instance.create.end'}]

    distilled_events_fc1_tenant_406904_missing_end = [{'os_distro': 'com.ubuntu',
                                           'event_type': 'compute.instance.create.start',
                                           'service': 'publisher-302689',
                                           'instance_type': '512MB Standard Instance',
                                           'tenant_id': '406904',
                                           'instance_flavor_id': '2',
                                           'hostname': 'server-462185',
                                           'host': 'publisher-302689',
                                           'instance_flavor': '512MB Standard Instance',
                                           'instance_id': '333-3b0f-4057-b377-b65131e8532e',
                                           'os_version': '12.04',
                                           'state': 'building',
                                           'os_architecture': 'x64',
                                           'timestamp': datetime.utcnow(),
                                           'request_id': 'req-d096b6de-f451-4d00-bff0-646a8c8a23c3',
                                           'message_id': '19701f6c-f51f-4ecb-85fb-7db40277627d'}]

    distilled_events_fc1_tenant_123456 = [{'os_distro': 'com.ubuntu',
                                           'event_type': 'compute.instance.create.start',
                                           'service': 'publisher-302689',
                                           'instance_type': '512MB Standard Instance',
                                           'tenant_id': '123456',
                                           'instance_flavor_id': '2',
                                           'hostname': 'server-462185',
                                           'host': 'publisher-302689',
                                           'instance_flavor': '512MB Standard Instance',
                                           'instance_id': '456-3b0f-4057-b377-b65131e8532e',
                                           'os_version': '12.04',
                                           'state': 'building',
                                           'os_architecture': 'x64',
                                           'timestamp': datetime.utcnow(),
                                           'request_id': 'req-d096b6de-f451-4d00-bff0-646a8c8a23c3',
                                           'message_id': '19701f6c-f51f-4ecb-85fb-7db40277627d'},
                                          {'os_distro': 'com.ubuntu',
                                           'message_id': '2ae21707-70ae-48a2-89c0-b08b11dc0b1a',
                                           'service': 'publisher-302689',
                                           'instance_type': '512MB Standard Instance',
                                           'tenant_id': '123456',
                                           'instance_flavor_id': '2',
                                           'hostname': 'server-462185',
                                           'host': 'publisher-302689',
                                           'instance_flavor': '512MB Standard Instance',
                                           'instance_id': '456-3b0f-4057-b377-b65131e8532e',
                                           'os_version': '12.04',
                                           'state': 'active',
                                           'os_architecture': 'x64',
                                           'timestamp': datetime.utcnow(),
                                              'request_id': 'req-d096b6de-f451-4d00-bff0-646a8c8a23c3',
                                              'launched_at': datetime.now(),
                                              'event_type': 'compute.instance.create.end'}]

    """ trigger defs for fire criteria 2 - looking for exists"""
    trig_def_fc2_rackspace_billing = [{'distinguished_by': ['instance_id',
                                                            {'timestamp': 'day'}],
                                       'fire_criteria': [{'event_type': 'compute.instance.exists'}],
                                       'match_criteria': [{'event_type': ['compute.instance.*',
                                                                          '!compute.instance.exists']},
                                                          {'event_type': 'compute.instance.exists',
                                                           'map_distingushed_by': {'timestamp': 'audit_period_beginning'}}],
                                       'name': 'rackspace_billing',
                                       'debug_level': 2,
                                       'expiration': '$last + 1h',
                                       'fire_pipeline': 'test_pipeline',
                                       'expire_pipeline': 'test_expire_pipeline'}]

    distilled_events_fc2_tenant_222333 = [{'os_distro': 'com.ubuntu',
                                           'event_type': 'compute.instance.create.start',
                                           'service': 'publisher-302689',
                                           'instance_type': '512MB Standard Instance',
                                           'tenant_id': '222333',
                                           'instance_flavor_id': '2',
                                           'hostname': 'server-462185',
                                           'host': 'publisher-302689',
                                           'instance_flavor': '512MB Standard Instance',
                                           'instance_id': '772b2f73-3b0f-4057-b377-b65131e8532e',
                                           'os_version': '12.04',
                                           'state': 'building',
                                           'os_architecture': 'x64',
                                           'timestamp': datetime.utcnow(),
                                           'request_id': 'req-d096b6de-f451-4d00-bff0-646a8c8a23c3',
                                           'message_id': '19701f6c-f51f-4ecb-85fb-7db40277627d'},
                                          {'os_distro': 'com.ubuntu',
                                           'message_id': '2ae21707-70ae-48a2-89c0-b08b11dc0b1a',
                                           'service': 'publisher-302689',
                                           'instance_type': '512MB Standard Instance',
                                           'tenant_id': '222333',
                                           'instance_flavor_id': '2',
                                           'hostname': 'server-462185',
                                           'host': 'publisher-302689',
                                           'instance_flavor': '512MB Standard Instance',
                                           'instance_id': '772b2f73-3b0f-4057-b377-b65131e8532e',
                                           'os_version': '12.04',
                                           'state': 'active',
                                           'os_architecture': 'x64',
                                           'timestamp': datetime.utcnow(),
                                              'request_id': 'req-d096b6de-f451-4d00-bff0-646a8c8a23c3',
                                              'launched_at': datetime.now(),
                                              'event_type': 'compute.instance.create.end'},
                                          {'os_distro': 'com.ubuntu',
                                           'message_id': '2ae21707-70ae-48a2-89c0-b08b11dc0b1a',
                                           'service': 'publisher-302689',
                                           'instance_type': '512MB Standard Instance',
                                           'tenant_id': '222333',
                                           'instance_flavor_id': '2',
                                           'hostname': 'server-462185',
                                           'host': 'publisher-302689',
                                           'instance_flavor': '512MB Standard Instance',
                                           'instance_id': '772b2f73-3b0f-4057-b377-b65131e8532e',
                                           'os_version': '12.04',
                                           'state': 'active',
                                           'os_architecture': 'x64',
                                           'timestamp': datetime.utcnow(),
                                              'request_id': 'req-d096b6de-f451-4d00-bff0-646a8c8a23c3',
                                              'launched_at': datetime.utcnow(),
                                              'event_type': 'compute.instance.exists'}]
    distilled_events_fc2_tenant_500001 = [{'os_distro': 'com.ubuntu',
                                           'event_type': 'compute.instance.create.start',
                                           'service': 'publisher-302689',
                                           'instance_type': '512MB Standard Instance',
                                           'tenant_id': '500001',
                                           'instance_flavor_id': '2',
                                           'hostname': 'server-462185',
                                           'host': 'publisher-302689',
                                           'instance_flavor': '512MB Standard Instance',
                                           'instance_id': '772b2f73-3b0f-4057-b377-b65131e8532e',
                                           'os_version': '12.04',
                                           'state': 'building',
                                           'os_architecture': 'x64',
                                           'timestamp': datetime.utcnow(),
                                           'request_id': 'req-d096b6de-f451-4d00-bff0-646a8c8a23c3',
                                           'message_id': '19701f6c-f51f-4ecb-85fb-7db40277627d'},
                                          {'os_distro': 'com.ubuntu',
                                           'message_id': '2ae21707-70ae-48a2-89c0-b08b11dc0b1a',
                                           'service': 'publisher-302689',
                                           'instance_type': '512MB Standard Instance',
                                           'tenant_id': '500001',
                                           'instance_flavor_id': '2',
                                           'hostname': 'server-462185',
                                           'host': 'publisher-302689',
                                           'instance_flavor': '512MB Standard Instance',
                                           'instance_id': '772b2f73-3b0f-4057-b377-b65131e8532e',
                                           'os_version': '12.04',
                                           'state': 'active',
                                           'os_architecture': 'x64',
                                           'timestamp': datetime.utcnow(),
                                              'request_id': 'req-d096b6de-f451-4d00-bff0-646a8c8a23c3',
                                              'launched_at': datetime.utcnow(),
                                              'event_type': 'compute.instance.create.end'},
                                          {'os_distro': 'com.ubuntu',
                                           'message_id': '2ae21707-70ae-48a2-89c0-b08b11dc0b1a',
                                           'service': 'publisher-302689',
                                           'instance_type': '512MB Standard Instance',
                                           'tenant_id': '500001',
                                           'instance_flavor_id': '2',
                                           'hostname': 'server-462185',
                                           'host': 'publisher-302689',
                                           'instance_flavor': '512MB Standard Instance',
                                           'instance_id': '772b2f73-3b0f-4057-b377-b65131e8532e',
                                           'os_version': '12.04',
                                           'state': 'active',
                                           'os_architecture': 'x64',
                                           'timestamp': datetime.utcnow(),
                                              'request_id': 'req-d096b6de-f451-4d00-bff0-646a8c8a23c3',
                                              'event_type': 'compute.instance.exists'}]

    def __init__(self, kafka_url, group, event_topic, fetch_size_bytes,
                 buffer_size, max_buffer_size, winchester_config):
        self.kafka_url = kafka_url
        self.group = group
        self.topic = event_topic
        self.winchester_config = winchester_config
        self.kafka = KafkaClient(self.kafka_url)
        self.consumer = SimpleConsumer(self.kafka,
                                       group,
                                       event_topic,
                                       auto_commit=True,
                                       # auto_commit_every_n=None,
                                       # auto_commit_every_t=None,
                                       # iter_timeout=1,
                                       fetch_size_bytes=fetch_size_bytes,
                                       buffer_size=buffer_size,
                                       max_buffer_size=max_buffer_size)

        self.consumer.seek(0, 2)
        # self.consumer.provide_partition_info()

        self.config = ConfigManager.load_config_file(winchester_config)
        self.trigger_manager = TriggerManager(self.config)

    def consume_raw(self):
        ''' Reads raw Openstack events from the raw-events topic.
            The winchester config defines a path to event_definitions.yaml
            which contains the transform instructions.  (We could have a 
            standard Openstack transform similar to what Rackspace uses.)
            The TriggerManager add_notifications call will transform the events
            first and then do the normal processing. '''
        for message in self.consumer:
            decoded = json.loads(message.message.value)
            self.trigger_manager.add_notification(decoded)

    def consume_transformed(self):
        ''' Have not tested this with the API yet, will change kafka consumption (like Persister). '''
        for message in self.consumer:
            sub_message = message[1].message
            envelop_str = sub_message.value

            envelope = simplejson.loads(envelop_str)
            event = envelope['event']

            if 'timestamp' in event:
                event['timestamp'] = datetime.strptime(
                    event['timestamp'],
                    "%Y-%m-%dT%H:%M:%S.%f+00:00")

            if 'when' in event:
                event['when'] = datetime.strptime(
                    event['when'],
                    "%Y-%m-%dT%H:%M:%S.%f+00:00")

            # add them to winchester TriggerManager
            self.trigger_manager.add_event(event)

    def _add_unique_event(self, e):
        ''' make the static test data contain unique message id's '''
        e['message_id'] = uuid.uuid4()
        self.trigger_manager.add_event(e)

    def add_test_distilled_events(self):
        # add trigger def
        self.trigger_manager.add_trigger_definition(
            EventProcessor.trig_def_fc1_tenant406904_filter)
        
        # add trigger def
        self.trigger_manager.add_trigger_definition(
            EventProcessor.trig_def_fc1_tenant123456_filter)

        #self.trigger_manager.add_trigger_definition(EventProcessor.trig_def_fc1)
        # test sending events
        for e in EventProcessor.distilled_events_fc1_tenant_406904:
            self._add_unique_event(e)
        for e in EventProcessor.distilled_events_fc1_tenant_123456:
            self._add_unique_event(e)
        for e in EventProcessor.distilled_events_fc1_tenant_406904_missing_end:
            self._add_unique_event(e)

        for e in EventProcessor.distilled_events_fc2_tenant_222333:
            self._add_unique_event(e)

        # delete trigger def
        #self.trigger_manager.delete_trigger_definition('fc1_trigger_123456')

        #self.trigger_manager.add_trigger_definition(EventProcessor.trig_def_fc2_rackspace_billing)
        #for e in EventProcessor.distilled_events_fc1_tenant_123456:
        #    self._add_unique_event(e)
        #for e in EventProcessor.distilled_events_fc1_tenant_406904:
        #    self._add_unique_event(e)

        #for e in EventProcessor.distilled_events_fc2_tenant_222333:
        #    self._add_unique_event(e)
        #for e in EventProcessor.distilled_events_fc2_tenant_500001:
        #    self._add_unique_event(e)


    def run(self):
        ''' The Event Processor needs to initialize the TriggerManager with
        Trigger Defs from the DB at startup.  It will read the stream-def-events kafka topic for
        the addition/deletion of stream-defs from the API.  It will read the transformed-events
        kafka topic for distilled event processing.  '''

        # self.consume_raw()

        self.add_test_distilled_events()
        time.sleep(120)
