# (C) Copyright 2015 Hewlett Packard Enterprise Development Company LP
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.
from oslo_config import cfg

winchester_opts = [
    cfg.StrOpt('winchester_config', default='/etc/winchester.yaml',
               help='Path to the winchester.yaml file.')]
winchester_group = cfg.OptGroup(name='winchester', title='winchester')

pipeline_proc_opts = [cfg.IntOpt('number', default=1,
                      help='The number of processes to start.')]
pipeline_proc_group = cfg.OptGroup(name='pipeline_processor',
                                   title='pipeline_processor')

event_proc_opts = [cfg.IntOpt('number', default=1,
                   help='The number of processes to start.')]
event_proc_group = cfg.OptGroup(name='event_processor',
                                title='event_processor')

logging_opts = [
    cfg.StrOpt('level', default='INFO'),
    cfg.StrOpt('file', default='/var/log/monasca/monasca-events-engine.log'),
    cfg.StrOpt('size', default=10485760),
    cfg.StrOpt('backup', default=5),
    cfg.StrOpt('kazoo', default="WARN"),
    cfg.StrOpt('kafka', default="WARN"),
    cfg.StrOpt('iso8601', default="WARN"),
    cfg.StrOpt('statsd', default="WARN")]
logging_group = cfg.OptGroup(name='logging', title='logging')

kafka_opts = [
    cfg.StrOpt('url', help='Address to kafka server. For example: '
               'url=192.168.10.4:9092'),
    cfg.StrOpt('events_topic', default='raw-events',
               help='The topic that events will be read from.'),
    cfg.StrOpt('event_group', default='monasca-event',
               help='The group name for reading events.'),
    cfg.StrOpt('stream_def_topic', default='stream-definitions',
               help='The topic for stream definition events.'),
    cfg.StrOpt('stream_def_group', default='streams_1',
               help='The event processor '
               'group for stream defs for this server'),
    cfg.StrOpt('stream_def_pipe_group', default='streams_pipe_1',
               help='The group for stream defs for this server'),
    cfg.StrOpt('notifications_topic', default='stream-notifications',
               help='The topic for sending notification events.'),
    cfg.StrOpt('transformed_events_topic', default='transformed-events',
               help='The topic for reading transformed events.')]
kafka_group = cfg.OptGroup(name='kafka', title='kafka')

zookeeper_opts = [
    cfg.StrOpt('url', default='notification-events',
               help='The topic for sending notification events.')]
zookeeper_group = cfg.OptGroup(name='zookeeper', title='zookeeper')

mysql_opts = [
    cfg.StrOpt('database_name'),
    cfg.StrOpt('hostname'),
    cfg.StrOpt('username'),
    cfg.StrOpt('password')]
mysql_group = cfg.OptGroup(name='mysql', title='mysql')


def register_logging_opts(conf):
    conf.register_group(logging_group)
    conf.register_opts(logging_opts, logging_group)


def register_mysql_opts(conf):
    conf.register_group(mysql_group)
    conf.register_opts(mysql_opts, mysql_group)


def register_kafka_opts(conf):
    conf.register_group(kafka_group)
    conf.register_opts(kafka_opts, kafka_group)


def register_winchester_opts(conf):
    conf.register_group(winchester_group)
    conf.register_opts(winchester_opts, winchester_group)


def register_event_processor_opts(conf):
    conf.register_group(event_proc_group)
    conf.register_opts(event_proc_opts, event_proc_group)


def register_pipeline_processor_opts(conf):
    conf.register_group(pipeline_proc_group)
    conf.register_opts(pipeline_proc_opts, pipeline_proc_group)


def register_zookeeper_opts(conf):
    conf.register_group(zookeeper_group)
    conf.register_opts(zookeeper_opts, zookeeper_group)
