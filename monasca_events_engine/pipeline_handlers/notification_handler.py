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
import json
import logging

import kafka
from monasca_events_engine.common.repositories.mysql.streams_repository \
    import StreamsRepository
from monasca_events_engine import stream_helpers as sh
from oslo_config import cfg
from winchester.pipeline_handler import PipelineHandlerBase

log = logging.getLogger(__name__)


def date_handler(obj):
    return obj.isoformat() if hasattr(obj, 'isoformat') else obj


def build_streams_notification_message(env, events, action_type='FIRE'):
    message = None
    if len(events) > 0:
        stream_name = sh.stream_def_name(env['stream_name'])
        tenant_id = events[0]['_tenant_id']
        streams_repo = StreamsRepository()
        stream_definition_rows = (
            streams_repo.get_stream_definitions(
                tenant_id, stream_name))
        for row in stream_definition_rows:
            if action_type.upper() == 'FIRE':
                action_id = row['fire_actions']
            else:
                action_id = row['expire_actions']
            message = {'tenant_id': tenant_id,
                       'stream_def': {
                           'name': stream_name,
                           'id': row['id'],
                           'description': row['description'],
                           'actions_enabled': row['actions_enabled'],
                           'action_type': action_type,
                           'action_id': action_id},
                       'events': []}
            for e in events:
                message['events'].append(e)
    return message


def send_stream_notification(message):
    try:
        _kafka = kafka.client.KafkaClient(cfg.CONF.kafka.url)
        producer = kafka.producer.SimpleProducer(_kafka)
    except Exception as e:
        log.error("Kafka producer cannot be created for url {}".format(
            cfg.CONF.kafka.url))
        raise e
    try:
        producer.send_messages(
            cfg.CONF.kafka.notifications_topic, message)
    except Exception as e:
        log.exception("Error sending message to Kafka topic {}".format(
            cfg.CONF.kafka.notifications_topic))
        raise e


class NotificationHandler(PipelineHandlerBase):
    """NotificationHandler

       A separate instance is created per fired stream
    """
    def handle_events(self, events, env):
        self.events = events
        self.env = env

        return events

    def commit(self):
        # send stream-notifications message
        log.debug("NotificationHandler in commit")
        emsg = ', '.join("%s: %s" % (event['event_type'], event['message_id'])
                         for event in self.events)
        log.info(
            "stream name: %s, id: %s recv: %s events: \n%s " %
            (self.env['stream_name'], self.env['stream_id'],
             len(self.events), emsg))

        message = build_streams_notification_message(
            self.env, self.events, 'FIRE')
        message_str = json.dumps(
            message, default=date_handler, ensure_ascii=False).encode('utf8')
        send_stream_notification(message_str)

    def rollback(self):
        log.debug("NotificationHandler in rollback")
        pass


class NotificationExpireHandler(PipelineHandlerBase):
    """NotificationExpireHandler

       A separate instance is created per expired stream.
    """
    def handle_events(self, events, env):
        self.events = events
        self.env = env

        return events

    def commit(self):
        # send stream-notifications message
        log.debug("NotificationExpireHandler in commit")
        emsg = ', '.join("%s: %s" % (event['event_type'], event['message_id'])
                         for event in self.events)
        log.info(
            "stream name: %s, id: %s recv: %s events: \n%s " %
            (self.env['stream_name'], self.env['stream_id'],
             len(self.events), emsg))

        message = build_streams_notification_message(
            self.env, self.events, 'EXPIRE')
        message_str = json.dumps(
            message, default=date_handler, ensure_ascii=False).encode('utf8')
        send_stream_notification(message_str)

    def rollback(self):
        log.debug("NotificationExpireHandler in rollback")
        pass
