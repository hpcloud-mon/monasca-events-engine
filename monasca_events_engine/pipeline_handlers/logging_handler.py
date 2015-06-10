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

log = logging.getLogger(__name__)


class LoggingHandler(PipelineHandlerBase):

    def handle_events(self, events, env):
        self.events = events
        self.env = env
        return events

    def commit(self):
        log.debug("LoggingHandler in commit")
        emsg = ', '.join("%s: %s" % (event['event_type'], event['message_id'])
                         for event in self.events)
        log.info(
            "stream name: %s, id: %s recv: %s events: \n%s " %
            (self.env['stream_name'], self.env['stream_id'],
             len(self.events), emsg))
        for event in self.events:
            print (event)

    def rollback(self):
        pass
