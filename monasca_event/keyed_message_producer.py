from kafka import KafkaClient, KeyedProducer, HashedPartitioner, RoundRobinPartitioner
from kafka.producer import Producer
import time
import json


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
