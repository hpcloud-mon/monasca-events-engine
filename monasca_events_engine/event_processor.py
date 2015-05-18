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

import iso8601
import json
from kafka import KafkaClient
from kafka import SimpleConsumer
import logging
import threading

from monasca_events_engine.event_processor_base import EventProcessorBase
import monascastatsd
from winchester.config import ConfigManager
from winchester.trigger_manager import TriggerManager

log = logging.getLogger(__name__)


class EventProcessor(EventProcessorBase):

    """EventProcessor

    The EventProcessor reads distilled events from the kafka
    'transformed-events' topic, and adds them to the stacktach winchester
    TriggerManager. Adding distilled events to the TriggerManager adds all
    distilled events into the Mysql DB.  The EventProcessor reads
    stream-definitions from the kafka 'stream-definitions'
    topic and adds them to the stacktach TriggerManager.
    The TriggerManager keeps temporary streams of events in Mysql -
    filtering by 'match criteria' and grouping by 'distinguished by'
    for each stream definition.  The streams are deleted when the fire
    criteria has been met.
    """

    def __init__(self, conf):
        super(EventProcessor, self).__init__(conf)
        self.winchester_config = conf.winchester.winchester_config
        self.config_mgr = ConfigManager.load_config_file(
            self.winchester_config)
        self.trigger_manager = TriggerManager(self.config_mgr)
        self.group = conf.kafka.stream_def_group
        self.tm_lock = threading.Lock()

    def event_consumer(self, conf, lock, trigger_manager):
        kafka_url = conf.kafka.url
        group = conf.kafka.event_group
        # read from the 'transformed_events_topic' in the future
        topic = conf.kafka.events_topic
        kafka = KafkaClient(kafka_url)
        fetch_size = conf.kafka.events_fetch_size_bytes
        buffer_size = conf.kafka.events_buffer_size
        max_buffer = conf.kafka.events_max_buffer_size
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
        events_consumed = statsd.get_counter('events_consumed')
        events_persisted = statsd.get_counter('events_persisted')

        for e in consumer:
            log.debug('Received an event')
            events_consumed.increment()
            offset, message = e
            envelope = json.loads(message.value)
            event = envelope['event']
            # convert iso8601 string to a datetime for winchester
            # Note: the distiller knows how to convert these based on
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
            try:
                trigger_manager.add_event(event)
                events_persisted.increment()
            except Exception as e:
                log.exception(e)
            lock.release()

    def run(self):
        """Initialize and start threads.

        The Event Processor initializes the TriggerManager with
        Trigger Defs from the DB at startup.  It reads the
        stream-def-events kafka topic for the addition/deletion of
        stream-defs from the API.  It reads the transformed-events
        kafka topic for distilled event processing.
        """

        # read stream-definitions from DB at startup and add
        stream_defs = self.stream_defs_from_database()
        if len(stream_defs) > 0:
            log.debug(
                'Loading {} stream definitions from the DB at startup'.format(
                    len(stream_defs)))
            self.trigger_manager.add_trigger_definition(stream_defs)

        # start threads
        self.stream_def_thread = threading.Thread(
            name='stream_defs',
            target=self.stream_definition_consumer,
            args=(self.conf, self.tm_lock, self.group, self.trigger_manager,))

        self.event_thread = threading.Thread(
            name='events',
            target=self.event_consumer,
            args=(self.conf, self.tm_lock, self.trigger_manager,))

        log.debug('Starting stream_defs and events threads')
        self.stream_def_thread.start()
        self.event_thread.start()

        self.stream_def_thread.join()
        self.event_thread.join()
        log.debug('Exiting')
