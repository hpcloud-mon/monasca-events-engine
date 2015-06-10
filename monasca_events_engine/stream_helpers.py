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
import string

log = logging.getLogger(__name__)

DEFAULT_FIRE_HANDLER = 'notification_fire_pipeline'
DEFAULT_EXPIRE_HANDLER = 'notification_expire_pipeline'


def stream_unique_name(stream):
    # format for winchester stream
    return stream['name'] + '_' + stream['tenant_id']

def stream_def_name(name):
    # format for monasca stream_definition
    response_name = name
    slist = string.rsplit(name,'_',1)
    if len(slist) > 0:
        response_name = slist[0]
    return response_name

def stream_def_to_winchester_format(stream):
    if 'select' in stream:
        stream['match_criteria'] = stream['select']
    else:
        # DB uses 'select_by' select is keyword
        stream['match_criteria'] = stream['select_by']

    stream['distinguished_by'] = stream['group_by']
    del stream['group_by']
    stream['name'] = stream_unique_name(stream)
    stream['fire_pipeline'] = DEFAULT_FIRE_HANDLER
    stream['expire_pipeline'] = DEFAULT_EXPIRE_HANDLER
    # expiration in python timex package format
    # convert from milliseconds to microseconds, since no ms support
    stream['expiration'] = '$first + {}us'.format(stream['expiration'] * 1000)
    if 'select' in stream:
        del stream['select']
    if 'select_by' in stream:
        del stream['select_by']
    if 'tenant_id' in stream:
        del stream['tenant_id']
    if 'stream_definition_id' in stream:
        del stream['stream_definition_id']
    if 'expire_actions' in stream:
        del stream['expire_actions']
    if 'fire_actions' in stream:
        del stream['fire_actions']
    if 'id' in stream:
        del stream['id']
    if 'actions_enabled' in stream:
        del stream['actions_enabled']
    if 'description' in stream:
        del stream['description']
    if 'created_at' in stream:
        del stream['created_at']
    if 'deleted_at' in stream:
        del stream['deleted_at']
    if 'updated_at' in stream:
        del stream['updated_at']
    return stream
