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

import json
import logging
import logging.config
import notigen
import sys
import time
import yaml
from datetime import datetime
import datetime
from keyed_message_producer import KeyedMessageProducer
from events import OpenstackEvent, Event, EventEnvelope


""" Generates nova events and writes them to kafka.

    This driver generates notigen nova events,
    converts them to the Event and EventEnvelope format that the 
    monasca API uses, and writes the EventEnvelopes to the 
    kafka raw-events topic.
"""

log = logging.getLogger(__name__)


def build_event_envelope(nova_event):
    event = Event()
    event.fromOpenstackEvent(nova_event)
    meta = {}
    meta['tenantId'] = nova_event[OpenstackEvent.TENANT_ID]
    # NOTE: the payload section is not consistently used.
    # meta['region'] = nova_event[OpenstackEvent.PAYLOAD][OpenstackEvent.REGION]
    creation_time = int(time.time())
    event_envelope = EventEnvelope(event, creation_time, **meta)
    return event_envelope.as_json()
    

def main(argv=None):
    rate = 10
    hours_to_run = 1
    realtime = 0
    config_file = '/etc/monasca/monasca_event.yaml'
    
    if argv is None:
        argv = sys.argv
    if len(argv) == 2:
        config_file = argv[1]
    elif len(argv) == 3:
        config_file = argv[1]
        rate = int(argv[2])
    elif len(argv) == 4:
        config_file = argv[1]
        rate = int(argv[2])
        hours_to_run = int(argv[3])
    elif len(argv) == 5:
        config_file = argv[1]
        rate = int(argv[2])
        hours_to_run = int(argv[3])
        realtime = int(argv[4])    
    else:
        print("Usage: " + argv[0] + " [config_file] <rate> <hours> <realtime 1/0>")
        print("")
        print("    config_file -(required) Full path of the monasca_event.yaml")
        print("    rate - (opt) Operations per hour. 10 is the default")
        print("    hours - (opt) Max number of hours to run. 1 is the default")
        print("    realtime - (opt) Default is false. When false the tick is moved every time events are processed.")
        return 1

    print("config_file dir = ", config_file)
    print("rate = ", rate)
    print("hours = ", hours_to_run)
    print("realtime = ", realtime)
    config = yaml.load(open(config_file, 'r'))

    # Setup logging
    logging.config.dictConfig(config['logging'])

    ## Start
    try:
        
        # init kafka producer
        producer = KeyedMessageProducer()
        producer.connect(config['kafka']['url']) 
        
        # rate is the operations per hour
        g = notigen.EventGenerator(config['notigen']['templatedir'],rate)
        now = datetime.datetime.utcnow()
        end = now + datetime.timedelta(hours=hours_to_run)
        nevents = 0
        sendcount = 0
        while now < end:
            events = g.generate(now)
            if events:
                nevents += len(events)
                for nova_event in events:
                    print ("the nova notigen event")
                    print (json.dumps(nova_event))
                    event_envelope = build_event_envelope(nova_event)
                    # send to kafka
                    print ("the envelope:")
                    print (event_envelope)
                    key = nova_event[OpenstackEvent.EVENT_TIMESTAMP]
                    #producer.send_message(config['kafka']['events_topic'], key, event_envelope)
                    producer.send_message(config['kafka']['events_topic'], key, json.dumps(nova_event))
                    sendcount += 1
            if realtime:
                now = datetime.datetime.utcnow()
            else:
                now = g.move_to_next_tick(now)
        print("sent: ", sendcount)
        print("nevents: " , nevents)
    except Exception as e:
        print e
        log.exception('Error! Exiting.', e)


if __name__ == "__main__":
    sys.exit(main())
