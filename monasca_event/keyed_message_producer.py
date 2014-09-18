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

from kafka import KafkaClient, KeyedProducer, HashedPartitioner, RoundRobinPartitioner
from kafka.producer import Producer


class KeyedMessageProducer():
    
    """ This is the kafka implementation of KeyedMessageProducer. """

    # options for partitioning by key
    HASHED = 1
    ROUND_ROBIN = 2
    
    def __init__(self, partition_by = HASHED):
        self.partition_by = partition_by
    
    def connect(self, url):
        kafka = KafkaClient(url)
        if self.partition_by == self.__class__.HASHED:
            self.producer = KeyedProducer(kafka, partitioner=HashedPartitioner,
                 async=False,
                 req_acks=Producer.ACK_AFTER_LOCAL_WRITE,
                 ack_timeout=Producer.DEFAULT_ACK_TIMEOUT)
        else:
            self.producer = KeyedProducer(kafka, partitioner=RoundRobinPartitioner,
                 async=False,
                 req_acks=Producer.ACK_AFTER_LOCAL_WRITE,
                 ack_timeout=Producer.DEFAULT_ACK_TIMEOUT,)

    def send_message(self, topic, key, message):  
        self.producer.send(topic, key, message)
