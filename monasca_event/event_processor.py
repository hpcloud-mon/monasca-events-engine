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
import iso8601
import json
from kafka import KafkaClient, SimpleConsumer
import logging
import simplejson
import time
import threading
import uuid


from winchester.config import ConfigManager
from winchester.trigger_manager import TriggerManager

log = logging.getLogger(__name__)


DEFAULT_FIRE_HANDLER = 'test_pipeline'
DEFAULT_EXPIRE_HANDLER = 'test_expire_pipeline'


def stream_unique_name(stream):
    return stream['name'] + '_' + stream['tenant_id']


def stream_def_to_winchester_format(stream):
    stream['match_criteria'] = stream['select']
    del stream['select']
    stream['distinguished_by'] = stream['group_by']
    del stream['group_by']
    stream['name'] = stream_unique_name(stream)
    del stream['tenant_id']
    stream['fire_pipeline'] = DEFAULT_FIRE_HANDLER
    stream['expire_pipeline'] = DEFAULT_EXPIRE_HANDLER
    del stream['stream_definition_id']
    # expiration must be in python timex package format
    # convert from milliseconds to microseconds, since no ms support
    stream['expiration'] = '$first + {}us'.format(stream['expiration'] * 1000)
    slist = list()
    slist.append(stream)
    return slist


def stream_definition_consumer(kafka_config, lock, trigger_manager):
    kafka_url = kafka_config['url']
    group = kafka_config['stream_def_group']
    topic = kafka_config['stream_def_topic']
    kafka = KafkaClient(kafka_url)
    consumer = SimpleConsumer(kafka,
                              group,
                              topic,
                              auto_commit=True,
                              # auto_commit_every_n=None,
                              # auto_commit_every_t=None,
                              # iter_timeout=1,
                              fetch_size_bytes=kafka_config['events_fetch_size_bytes'],
                              buffer_size=kafka_config['events_buffer_size'],
                              max_buffer_size=kafka_config['events_max_buffer_size'])

    consumer.seek(0, 2)

    for s in consumer:
        offset, message = s
        stream_def = json.loads(message.value)

        if 'stream-definition-created' in stream_def:
            log.debug('Received a stream definition created event')
            stream_create = stream_def_to_winchester_format(
                stream_def['stream-definition-created'])
            lock.acquire()
            trigger_manager.add_trigger_definition(stream_create)
            lock.release()
        elif 'stream-definition-deleted' in stream_def:
            log.debug('Received a stream-definition-deleted event')
            name = stream_unique_name(stream_def['stream-definition-deleted'])
            lock.acquire()
            trigger_manager.delete_trigger_definition(name)
            lock.release()
        else:
            log.error('Unknown event received on stream_def_topic')


def event_consumer(kafka_config, lock, trigger_manager):
    kafka_url = kafka_config['url']
    group = kafka_config['event_group']
    # read from the 'transformed_events_topic' in the future
    # reading events sent from API POST Event now
    topic = kafka_config['events_topic']
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

    for e in consumer:
        log.debug('Received an event')
        offset, message = e
        envelope = json.loads(message.value)
        event = envelope['event']
        # convert iso8601 string to a datetime for winchester
        # Note: the distiller knows how to convert these, based on
        # event_definitions.yaml
        if 'timestamp' in event:
            event['timestamp'] = iso8601.parse_date(
                event['timestamp'],
                default_timezone=None)
        if 'launched_at' in event:
            event['launched_at'] = iso8601.parse_date(
                event['launched_at'],
                default_timezone=None)

        lock.acquire()
        trigger_manager.add_event(event)
        lock.release()


class EventProcessor():

    """  EventProcessor

    The EventProcessor reads distilled events from the kafka 'transformed-events' topic, and adds them to the
    stacktach winchester TriggerManager. Adding distilled events to the TriggerManager adds all distilled 
    events into the Mysql DB.  The EventProcessor reads stream-definitions from the kafka 'stream-definitions' 
    topic and adds them to the stacktach TriggerManager.
    The TriggerManager keeps temporary streams of events in Mysql - filtering by 'match criteria'
    and grouping by 'distinguished by' for each stream definition.
    The streams are deleted when the fire criteria has been met.
    """

    def __init__(self, kafka_config, winchester_config):
        self.kafka_config = kafka_config
        self.winchester_config = winchester_config
        self.config_mgr = ConfigManager.load_config_file(
            self.winchester_config)

    # consume_raw is not used, but we could do the transform once here for
    # openstack events
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

    def run(self):
        ''' The Event Processor needs to initialize the TriggerManager with
        Trigger Defs from the DB at startup.  It will read the stream-def-events kafka topic for
        the addition/deletion of stream-defs from the API.  It will read the transformed-events
        kafka topic for distilled event processing.
        '''

        # Initialization
        self.tm_lock = threading.Lock()
        self.trigger_manager = TriggerManager(self.config_mgr)

        # TODO read stream-definitions from DB at startup and add

        self.stream_def_thread = threading.Thread(name='stream_defs',
                                                  target=stream_definition_consumer,
                                                  args=(self.kafka_config, self.tm_lock, self.trigger_manager,))

        self.event_thread = threading.Thread(name='events',
                                             target=event_consumer,
                                             args=(self.kafka_config, self.tm_lock, self.trigger_manager,))

        self.stream_def_thread.start()
        self.event_thread.start()

        self.stream_def_thread.join()
        self.event_thread.join()
        log.debug('Exiting')
