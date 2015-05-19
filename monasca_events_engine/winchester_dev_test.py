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

from datetime import datetime
import logging
import time
import uuid

from winchester.config import ConfigManager
from winchester.pipeline_manager import PipelineManager
from winchester.trigger_manager import TriggerManager


log = logging.getLogger(__name__)


class TriggerTest(object):

    """Trigger Test

    Adds Stream Definitions to the TriggerManager and PipelineManager
    classes.  Adds Fake distilled events to the TriggerManager and ensures
    the Fire and expire handlers will get called.  This test uses the
    winchester mysql DB.
    """

    trig_def_fc1 = [{'distinguished_by': ['instance_id'],
                     'fire_criteria': [
                         {'event_type': 'compute.instance.create.start'},
                         {'event_type': 'compute.instance.create.end'}],
                     'match_criteria': [
                         {'event_type': 'compute.instance.create.*'}],
                     'name': 'fc1_trigger',
                     'debug_level': 2,
                     'expiration': '$last + 1h',
                     'fire_pipeline': 'test_pipeline',
                     'expire_pipeline': 'test_expire_pipeline'}]

    trig_def_fc1_tenant406904_filter = [
        {'distinguished_by': ['instance_id'],
         'fire_criteria': [
             {'event_type': 'compute.instance.create.start'},
             {'event_type': 'compute.instance.create.end'}],
         'match_criteria': [
             {'traits': {'tenant_id': '406904'},
              'event_type': 'compute.instance.create.*'}],
         'name': 'trig_def_fc1_406904',
         'debug_level': 2,
         'expiration': '$first + 10s',
         'fire_pipeline': 'test_pipeline',
         'expire_pipeline': 'test_expire_pipeline'}]

    trig_def_fc1_tenant123456_filter = [
        {'distinguished_by': ['instance_id'],
         'fire_criteria': [
             {'event_type': 'compute.instance.create.start'},
             {'event_type': 'compute.instance.create.end'}],
         'match_criteria': [{'traits': {'tenant_id': '123456'},
                            'event_type': 'compute.instance.create.*'}],
         'name': 'fc1_trigger_123456',
         'debug_level': 2,
         'expiration': '$last + 24h',
         'fire_pipeline': 'test_pipeline',
         'expire_pipeline': 'test_expire_pipeline'}]

    """ distilled events to cause fire criteria """

    distilled_events_fc1_tenant_406904 = [
        {'os_distro': 'com.ubuntu',
         'event_type': 'compute.instance.create.start',
         'service': 'publisher-302689',
         'instance_type': '512MB Standard Instance',
         'tenant_id': '406904',
         'instance_flavor_id': '2',
         'hostname': 'server-462185',
         'host': 'publisher-302689',
         'instance_flavor': '512MB Standard Instance',
         'instance_id': '111-3b0f-4057-b377-b65131e8532e',
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
         'instance_id': '111-3b0f-4057-b377-b65131e8532e',
         'os_version': '12.04',
         'state': 'active',
         'os_architecture': 'x64',
         'timestamp': datetime.utcnow(),
         'request_id': 'req-d096b6de-f451-4d00-bff0-646a8c8a23c3',
         'launched_at': datetime.utcnow(),
         'event_type': 'compute.instance.create.end'}]

    distilled_events_fc1_tenant_406904_missing_end = [
        {'os_distro': 'com.ubuntu',
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

    distilled_events_fc1_tenant_123456 = [
        {'os_distro': 'com.ubuntu',
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
         'launched_at': datetime.utcnow(),
         'event_type': 'compute.instance.create.end'}]

    """ trigger defs for fire criteria 2 - looking for exists"""
    trig_def_fc2_rackspace_billing = [
        {'distinguished_by': ['instance_id', {'timestamp': 'day'}],
         'fire_criteria': [{'event_type': 'compute.instance.exists'}],
         'match_criteria': [{'event_type': ['compute.instance.*',
                                            '!compute.instance.exists']},
                            {'event_type': 'compute.instance.exists',
                             'map_distingushed_by': {
                                 'timestamp': 'audit_period_beginning'}}],
         'name': 'rackspace_billing',
         'debug_level': 2,
         'expiration': '$last + 1h',
         'fire_pipeline': 'test_pipeline',
         'expire_pipeline': 'test_expire_pipeline'}]

    distilled_events_fc2_tenant_222333 = [
        {'os_distro': 'com.ubuntu',
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
         'launched_at': datetime.utcnow(),
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

    def __init__(self, winchester_config):
        self.winchester_config = winchester_config

        self.config = ConfigManager.load_config_file(winchester_config)
        self.trigger_manager = TriggerManager(self.config)
        self.pipe = PipelineManager(self.config)

    def _add_unique_event(self, e):
        # make the static test data contain unique message id
        e['message_id'] = uuid.uuid4()
        self.trigger_manager.add_event(e)

    def add_test_stream_definitions(self):
        self.trigger_manager.add_trigger_definition(
            TriggerTest.trig_def_fc1_tenant406904_filter)
        self.trigger_manager.add_trigger_definition(
            TriggerTest.trig_def_fc1_tenant123456_filter)

        self.pipe.add_trigger_definition(
            TriggerTest.trig_def_fc1_tenant406904_filter)
        self.pipe.add_trigger_definition(
            TriggerTest.trig_def_fc1_tenant123456_filter)

    def add_distilled_events_to_fire(self):
        for e in TriggerTest.distilled_events_fc1_tenant_406904:
            self._add_unique_event(e)
        for e in TriggerTest.distilled_events_fc1_tenant_123456:
            self._add_unique_event(e)

    def add_distilled_events_to_expire(self):
        for e in TriggerTest.distilled_events_fc1_tenant_406904_missing_end:
            self._add_unique_event(e)

    def add_distilled_events_with_no_match(self):
        for e in TriggerTest.distilled_events_fc2_tenant_222333:
            self._add_unique_event(e)

    def check_for_expired_streams(self):
        stream_count = self.pipe.process_ready_streams(
            self.pipe.pipeline_worker_batch_size,
            expire=True)
        return stream_count

    def check_for_fired_streams(self):
        stream_count = self.pipe.process_ready_streams(
            self.pipe.pipeline_worker_batch_size)
        return stream_count

    def test_no_match(self):
        self.add_distilled_events_with_no_match()
        time.sleep(2)
        fired_count = self.check_for_fired_streams()
        expired_count = self.check_for_expired_streams()

        if (fired_count == 0 and expired_count == 0):
            print ("test_no_match: Success")
        else:
            print ("test_no_match: Failed")

    def test_fired(self):
        self.add_distilled_events_to_fire()
        time.sleep(3)
        fired_count = self.check_for_fired_streams()
        expired_count = self.check_for_expired_streams()
        if (expired_count == 0 and fired_count == 2):
            print ("test_fired: Success")
        else:
            print ("test_fired: Failed")

    def test_expired(self):
        self.add_distilled_events_to_expire()
        time.sleep(11)
        fired_count = self.check_for_fired_streams()
        expired_count = self.check_for_expired_streams()
        if (expired_count == 1 and fired_count == 0):
            print ("test_expired: Success")
        else:
            print ("test_expired: Failed")

# main
trigger_test = TriggerTest('../etc/winchester.yaml')
trigger_test.add_test_stream_definitions()

trigger_test.test_no_match()
trigger_test.test_fired()
trigger_test.test_expired()
