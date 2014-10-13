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
import kafka.client
import kafka.producer
from pipeline_handler import PipelineHandlerBase
import time
import json
from datetime import datetime

logger = logging.getLogger(__name__)

kafka_url = '192.168.10.4:9092'
kafka_group = 'monasca-event'
metrics_topic = 'metrics'
tenant_id = '497b8816dceb45718956bfe96640b83a'
region = 'useast'

class VmStartupTimeHandler(PipelineHandlerBase):

    def __init__(self):
        self._kafka = kafka.client.KafkaClient(kafka_url)
        self._producer = kafka.producer.SimpleProducer(self._kafka,
                                                       async=False,
                                                       req_acks=kafka.producer.SimpleProducer.ACK_AFTER_LOCAL_WRITE,
                                                       ack_timeout=2000)

    def handle_events(self, events, env):
        start_time = None
        end_time = None

        for event in events:
            if event['event_type'] == 'compute.instance.create.start':
                start_time = event['when']
            if event['event_type'] == 'compute.instance.create.end':
                end_time = event['when']

        if start_time == None or end_time == None:
            return events

        if end_time < start_time:
            return events

        startup_time = (end_time - start_time).total_seconds()

        metric = {
            'name': 'vm_startup_time_secs',
            'dimensions': {
                'flavor': 'small'
            },
            'timestamp': time.time(),
            'value': startup_time
        }

        envelope = {
            'metric': metric,
            'meta': {
                'tenantId': tenant_id,
                'region': region
            },
            'creation_time': datetime.now()
        }

        def date_handler(obj):
            return obj.isoformat() if hasattr(obj, 'isoformat') else obj

        message = json.dumps(envelope, default=date_handler)
        self._producer.send_messages(metrics_topic, message)
        return events

    def commit(self):
        pass

    def rollback(self):
        pass