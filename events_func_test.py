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
import json
import requests
import time
import uuid

from monascaclient import ksclient


events_base_url = "http://127.0.0.1:8082"
notifications_base_url = "http://192.168.10.4:8080"


event_compute_start = {
    'os_distro': 'com.ubuntu',
    'event_type': 'compute.instance.create.start',
    'service': 'publisher-302689',
    'instance_type': '512MB Standard Instance',
    'tenant_id': 'd2949c81659e405cb7824f0bc49487d6',
    'instance_flavor_id': '2',
    'hostname': 'server-462185',
    'host': 'publisher-302689',
    'instance_flavor': '512MB Standard Instance',
    'instance_id': '111-3b0f-4057-b377-b65131e8532e',
    'os_version': '12.04',
    'state': 'building',
    'os_architecture': 'x64',
    'timestamp': datetime.utcnow().isoformat(),
    'request_id': 'req-d096b6de-f451-4d00-bff0-646a8c8a23c3',
    'message_id': '19701f6c-f51f-4ecb-85fb-7db40277627d'}

event_compute_end = {'os_distro': 'com.ubuntu',
                     'message_id': '2ae21707-70ae-48a2-89c0-b08b11dc0b1a',
                     'service': 'publisher-302689',
                     'instance_type': '512MB Standard Instance',
                     'tenant_id': 'd2949c81659e405cb7824f0bc49487d6',
                     'instance_flavor_id': '2',
                     'hostname': 'server-462185',
                     'host': 'publisher-302689',
                     'instance_flavor': '512MB Standard Instance',
                     'instance_id': '111-3b0f-4057-b377-b65131e8532e',
                     'os_version': '12.04',
                     'state': 'active',
                     'os_architecture': 'x64',
                     'timestamp': datetime.utcnow().isoformat(),
                     'request_id': 'req-d096b6de-f451-4d00-bff0-646a8c8a23c3',
                     'launched_at': datetime.utcnow().isoformat(),
                     'event_type': 'compute.instance.create.end'}


def token():
    keystone = {
        'username': 'mini-mon',
        'password': 'password',
        'project': 'test',
        'auth_url': 'http://192.168.10.5:35357/v3'
    }
    ks_client = ksclient.KSClient(**keystone)
    return ks_client.token


def unique_event(e, tenant_id, instance_id=None):
    e['message_id'] = str(uuid.uuid1())
    # note: event post will change to add _tenant_id to the event
    e['tenant_id'] = tenant_id
    e['timestamp'] = datetime.utcnow().isoformat()
    if instance_id:
        e['instance_id'] = instance_id
    if 'launched_at' in e:
        e['launched_at'] = datetime.utcnow().isoformat()
    return e


def test_events_get(id=None, tenant_id=None):
    headers = {
        'X-Auth-User': 'mini-mon',
        'X-Auth-Token': token(),
        'X-Auth-Key': 'password',
        'Accept': 'application/json',
        'User-Agent': 'python-monascaclient',
        'Content-Type': 'application/json'}

    body = {}
    params = ""
    if id:
        params = id
    elif tenant_id:
        params = "?tenant_id={}".format(tenant_id)
    response = requests.get(url=events_base_url + "/v2.0/events" + params,
                            data=json.dumps(body),
                            headers=headers)

    assert response.status_code == 200
    print("GET /events success")
    return response


def test_stream_definition_post_no_trait(tenant_id, name):
    headers = {
        'X-Auth-User': 'mini-mon',
        'X-Auth-Key': 'password',
        'X-Auth-Token': token(),
        'Accept': 'application/json',
        'User-Agent': 'python-monascaclient',
        'Content-Type': 'application/json'}

    body = {}

    notif_resp = requests.get(
        url=notifications_base_url + "/v2.0/notification-methods",
        data=json.dumps(body), headers=headers)
    notif_dict = json.loads(notif_resp.text)
    action_id = str(notif_dict['elements'][0]['id'])

    body = {"fire_criteria": [{"event_type": "compute.instance.create.start"},
                              {"event_type": "compute.instance.create.end"}],
            "description": "provisioning duration",
            "name": name,
            "group_by": ["instance_id"],
            "expiration": 4000,
            "select": [{"event_type": "compute.instance.create.*"}],
            "fire_actions": [action_id],
            "expire_actions": [action_id]}

    response = requests.post(
        url=events_base_url + "/v2.0/stream-definitions",
        data=json.dumps(body),
        headers=headers)
    assert response.status_code == 201
    print("POST /stream-definitions no_trait success")
    return response


def test_stream_definition_post_existing_trait(tenant_id, name):
    headers = {
        'X-Auth-User': 'mini-mon',
        'X-Auth-Key': 'password',
        'X-Auth-Token': token(),
        'Accept': 'application/json',
        'User-Agent': 'python-monascaclient',
        'Content-Type': 'application/json'}

    body = {}

    notif_resp = requests.get(
        url=notifications_base_url + "/v2.0/notification-methods",
        data=json.dumps(body), headers=headers)
    notif_dict = json.loads(notif_resp.text)
    action_id = str(notif_dict['elements'][0]['id'])

    body = {"fire_criteria": [{"event_type": "compute.instance.create.start"},
                              {"event_type": "compute.instance.create.end"}],
            "description": "provisioning duration",
            "name": name,
            "group_by": ["instance_id"],
            "expiration": 4000,
            "select": [{"traits": {"hostname": "server-462185"},
                        "event_type": "compute.instance.create.*"}],
            "fire_actions": [action_id],
            "expire_actions": [action_id]}

    response = requests.post(
        url=events_base_url + "/v2.0/stream-definitions",
        data=json.dumps(body),
        headers=headers)
    assert response.status_code == 201
    print("POST /stream-definitions existing_trait success")
    return response


def test_stream_definition_post_invalid_trait(tenant_id, name):
    headers = {
        'X-Auth-User': 'mini-mon',
        'X-Auth-Key': 'password',
        'X-Auth-Token': token(),
        'Accept': 'application/json',
        'User-Agent': 'python-monascaclient',
        'Content-Type': 'application/json'}

    body = {}

    notif_resp = requests.get(
        url=notifications_base_url + "/v2.0/notification-methods",
        data=json.dumps(body), headers=headers)
    notif_dict = json.loads(notif_resp.text)
    action_id = str(notif_dict['elements'][0]['id'])

    body = {"fire_criteria": [{"event_type": "compute.instance.create.start"},
                              {"event_type": "compute.instance.create.end"}],
            "description": "provisioning duration",
            "name": name,
            "group_by": ["instance_id"],
            "expiration": 4000,
            "select": [{"traits": {"_tenant_id": "462185"},
                        "event_type": "compute.instance.create.*"}],
            "fire_actions": [action_id],
            "expire_actions": [action_id]}

    response = requests.post(
        url=events_base_url + "/v2.0/stream-definitions",
        data=json.dumps(body),
        headers=headers)
    assert response.status_code == 400
    print("POST /stream-definitions invalid_trait success")
    return response


def test_stream_definition_get(id=None, name=None):
    headers = {
        'X-Auth-User': 'mini-mon',
        'X-Auth-Key': 'password',
        'X-Auth-Token': token(),
        'Accept': 'application/json',
        'User-Agent': 'python-monascaclient',
        'Content-Type': 'application/json'}

    body = {}
    params = ""
    if id:
        params = id
    elif name:
        params = "?name={}".format(name)

    response = requests.get(
        url=events_base_url + "/v2.0/stream-definitions/" + params,
        data=json.dumps(body),
        headers=headers)
    assert response.status_code == 200
    print("GET /stream-definitions success")
    return response


def test_stream_definition_delete(id):
    headers = {
        'X-Auth-User': 'mini-mon',
        'X-Auth-Token': token(),
        'X-Auth-Key': 'password',
        'Accept': 'application/json',
        'User-Agent': 'python-monascaclient',
        'Content-Type': 'application/json'}

    body = {}

    response = requests.delete(
        url=events_base_url + "/v2.0/stream-definitions/{}".format(
            id),
        data=json.dumps(body),
        headers=headers)
    assert response.status_code == 204
    print("DELETE /stream-definitions success")


def test_post_event(e, tenant_id, instance_id=None):
    headers = {
        'X-Auth-User': 'mini-mon',
        'X-Auth-Key': 'password',
        'X-Auth-Token': token(),
        'Accept': 'application/json',
        'User-Agent': 'python-monascaclient',
        'Content-Type': 'application/json'}

    body = unique_event(e, tenant_id, instance_id)
    response = requests.post(
        url=events_base_url + "/v2.0/events",
        data=json.dumps(body),
        headers=headers)
    assert response.status_code == 204
    print("POST /events success")
    return response


def add_stream_defs():
    p1_resp = test_stream_definition_post_existing_trait(
        "d2949c81659e405cb7824f0bc49487d6", "existing_trait")
    p1_data = json.loads(p1_resp.text)
    test_stream_definition_get(id=p1_data['id'])

    p2_resp = test_stream_definition_post_no_trait(
        "d2949c81659e405cb7824f0bc49487d6", "no_trait")
    p2_data = json.loads(p2_resp.text)
    test_stream_definition_get(id=p2_data['id'])

    test_stream_definition_post_invalid_trait(
        "d2949c81659e405cb7824f0bc49487d6", "invalid_trait")

    print ("add stream definitions: success")


def add_events_to_fire():
    instance_id = str(uuid.uuid1())
    test_post_event(event_compute_start,
                    "d2949c81659e405cb7824f0bc49487d6",
                    instance_id)
    test_post_event(event_compute_end,
                    "d2949c81659e405cb7824f0bc49487d6",
                    instance_id)
    # then query the events
    g_resp = test_events_get(tenant_id="d2949c81659e405cb7824f0bc49487d6")
    g_data = json.loads(g_resp.text)
    # see if we got the ones we just added
    instance_cnt = 0
    for e in g_data:
        if e['data']['instance_id'] == instance_id:
            instance_cnt += 1
    assert instance_cnt >= 1
    print ("add events to fire: success")


def add_notigen_events_to_fire():
    instance_id = str(uuid.uuid1())
    notigen_event_start = []
    notigen_event_end = []

    for i in range(0, 5):
        u_start = unique_event(event_compute_start,
                               "406904",
                               instance_id)
        notigen_event_start.append(u_start)

        u_end = unique_event(event_compute_end,
                             "406904",
                             instance_id)
        notigen_event_end.append(u_end)

    headers = {
        'X-Auth-User': 'mini-mon',
        'X-Auth-Key': 'password',
        'X-Auth-Token': token(),
        'Accept': 'application/json',
        'User-Agent': 'python-monascaclient',
        'Content-Type': 'application/json'}

    response = requests.post(
        url=events_base_url + "/v2.0/events",
        data=json.dumps(notigen_event_start),
        headers=headers)
    assert response.status_code == 204

    response = requests.post(
        url=events_base_url + "/v2.0/events",
        data=json.dumps(notigen_event_end),
        headers=headers)
    assert response.status_code == 204

    # then query the events
    g_resp = test_events_get(tenant_id="406904")
    g_data = json.loads(g_resp.text)
    # see if we got the ones we just added
    instance_cnt = 0
    for e in g_data:
        if e['data']['instance_id'] == instance_id:
            instance_cnt += 1

    assert instance_cnt == 2
    print ("add notigen events to fire: success")


def add_events_to_expire():
    instance_id = str(uuid.uuid1())
    test_post_event(event_compute_start,
                    "d2949c81659e405cb7824f0bc49487d6",
                    instance_id)
    # then query the events
    g_resp = test_events_get(tenant_id="d2949c81659e405cb7824f0bc49487d6")
    g_data = json.loads(g_resp.text)
    # see if we got the ones we just added
    instance_cnt = 0
    for e in g_data:
        if e['data']['instance_id'] == instance_id:
            instance_cnt += 1
    assert instance_cnt >= 1
    print ("add events to expire: success")


def del_stream_defs():
    g_resp1 = test_stream_definition_get(name="existing_trait")
    g_data1 = json.loads(g_resp1.text)
    test_stream_definition_delete(g_data1[0]['id'])

    g_resp2 = test_stream_definition_get(name="no_trait")
    g_data2 = json.loads(g_resp2.text)
    test_stream_definition_delete(g_data2[0]['id'])

    print ("delete stream definitions: success")

add_stream_defs()
add_events_to_fire()
add_notigen_events_to_fire()
time.sleep(10)
add_events_to_expire()
time.sleep(20)
del_stream_defs()
