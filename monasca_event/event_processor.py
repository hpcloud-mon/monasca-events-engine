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

import kafka.client
import kafka.consumer
import kafka.producer
import logging
from base_processor import BaseProcessor
from datetime import datetime
from time import mktime

import json

# imports for oahu
import datetime
import notigen

from oahu import inmemory
from oahu import trigger_definition
from oahu import pipeline_callback
from oahu import criteria
from oahu import mongodb_driver
from oahu import pipeline

log = logging.getLogger(__name__)

NOTIGEN_TEMPLATE_DIR = '/Users/cindy/publicGithub/StackTach/notigen/templates'

class OutOfOrderException(Exception):
    pass

class TestCallback(pipeline_callback.PipelineCallback):
    """ implement PipelineCallback """
    def on_trigger(self, stream):
        print "Got: ", stream
        for event in stream.events:
            print event['event_type'], event['when']

class TestPipeline():
    """ TestPipeline generates events, intializes the pipeline with a driver and callback, and calls process_ready_streams(now) on the pipeline. """ 
    def _pipeline(self, driver, trigger_name, callback):
        p = pipeline.Pipeline(driver)

        driver.flush_all()
        if driver.get_num_active_streams(trigger_name) == 0:
            print ("after flush_all - no streams active")

        g = notigen.EventGenerator(NOTIGEN_TEMPLATE_DIR,100,0)
        now = datetime.datetime.utcnow()
        nevents = 0
        unique = set()
        while nevents < 5000:
            events = g.generate(now)
            if events:
                for event in events:
                    p.add_event(event)
                    unique.add(event["_context_request_id"])
                nevents += len(events)
            now = g.move_to_next_tick(now)

        num_streams = driver.get_num_active_streams(trigger_name)
        print ('num_streams = ',num_streams)
        now += datetime.timedelta(seconds=2)

        # These calls would be performed by separate processes that poll
        p.do_expiry_check(100,now)
        p.process_ready_streams(100,now)
        p.purge_streams(100)
        if len(unique) == callback.triggered:
            print ('triggered this many times: ', callback.triggered)
        if len(unique) == callback.streams:
            print ('this many streams: ', callback.streams)
        #self.assertEqual(len(unique), callback.triggered)
        #self.assertEqual(len(unique), len(callback.streams))
        #self.assertEqual(unique, callback.request_set)

    def _get_rules(self):
        """ create the stream rules """
        inactive = criteria.Inactive(60)
        callback = TestCallback()
        trigger_name = "request-id"
        by_request = trigger_definition.TriggerDefinition(trigger_name,
                                                          ["_context_request_id", ],
                                                          inactive, 
                                                          callback)
        rules = [by_request, ]

        return (rules, callback, trigger_name)

    # TODO(sandy): The drivers for these tests will come from a configuration
    # and simport'ed.

    def test_inmemory(self):
        rules, callback, trigger_name = self._get_rules()
        driver = inmemory.InMemoryDriver(rules)
        self._pipeline(driver, trigger_name, callback)
        
    def test_mongo(self):
        rules, callback, trigger_name = self._get_rules()
        driver = mongodb_driver.MongoDBDriver(rules)
        self._pipeline(driver, trigger_name, callback)

class MyEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, datetime.datetime):
            return int(mktime(obj.timetuple()))

        return json.JSONEncoder.default(self, obj)

class EventProcessor(BaseProcessor):
    def __init__(self, kafka_url, group, topic):
        """
        Init
        kafka_url, group, topic - kafka connection details
        """
        """
        self.topic = topic
        self.kafka = kafka.client.KafkaClient(kafka_url)
        self.consumer = kafka.consumer.SimpleConsumer(self.kafka, group, topic, auto_commit=True)
        self.consumer.seek(0, 2)
        self.consumer.provide_partition_info()  # Without this the partition is not provided in the response
        self.producer = kafka.producer.SimpleProducer(self.kafka,
                                              async=False,
                                              req_acks=kafka.producer.SimpleProducer.ACK_AFTER_LOCAL_WRITE,
                                              ack_timeout=2000)
         """

    def run(self):
        
        pipeline = TestPipeline()
        #pipeline.test_inmemory()
        pipeline.test_mongo()
        
