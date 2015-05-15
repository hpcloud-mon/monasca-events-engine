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
import logging

from winchester.pipeline_handler import PipelineHandlerBase

logger = logging.getLogger(__name__)


class LoggingHandler(PipelineHandlerBase):

    def handle_events(self, events, env):
        emsg = ', '.join("%s: %s" % (event['event_type'], event['message_id'])
                         for event in events)
        logger.info(
            "stream name: %s, id: %s recv: %s events: \n%s " %
            (env['stream_name'], env['stream_id'], len(events), emsg))
        for event in events:
            print (event)

        return events

    def commit(self):
        print ("LoggingHandler commit called!")

        pass

    def rollback(self):
        pass


class NotificationHandler(PipelineHandlerBase):
    '''NotificationHandler

    There is a separate instance of the handler for each
    stream.
    '''
    def handle_events(self, events, env):
        self.events = events
        self.env = env

        return events

    def commit(self):
        '''commit is where we process the events.

        The events will be sent to the notification
        engine via kafka message.
        '''
        print ("NotificationHandler!!! in commit")
        emsg = ', '.join("%s: %s" % (event['event_type'], event['message_id'])
                         for event in self.events)
        logger.info(
            "stream name: %s, id: %s recv: %s events: \n%s " %
            (self.env['stream_name'], self.env['stream_id'],
             len(self.events), emsg))
        pass

    def rollback(self):
        pass


class NotificationExpireHandler(PipelineHandlerBase):
    '''NotificationHandler

    There is a separate instance of the handler for each
    stream.
    '''
    def handle_events(self, events, env):
        self.events = events
        self.env = env

        return events

    def commit(self):
        '''commit is where we process the events.

        The events will be sent to the notification
        engine via kafka message.
        '''
        print ("NotificationExpireHandler!!! in commit")
        emsg = ', '.join("%s: %s" % (event['event_type'], event['message_id'])
                         for event in self.events)
        logger.info(
            "stream name: %s, id: %s recv: %s events: \n%s " %
            (self.env['stream_name'], self.env['stream_id'],
             len(self.events), emsg))
        pass

    def rollback(self):
        pass
