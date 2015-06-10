#!/usr/bin/env python
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

""" Monasca Events Engine

    This is the entry point for the monasca events engine.
    To use it pass in the path of the monasca_event.yaml file.
        python main.py [monasca_event.yaml]

    The EventProcessor process will read transformed events from kafka
    and add them to the stacktach-winchester TriggerManager.

    The PipelineProcessor process will start up the
    stacktach-winchester PipelineManager which loads handlers,
    periodically checks for ready and expired streams, and calls
    the handlers associated with the ready and expired streams.
"""

import logging
import multiprocessing
import os
import signal
import sys
import time

from monasca_events_engine.event_processor import EventProcessor
from monasca_events_engine.pipeline_processor import PipelineProcessor
from oslo_config import cfg

log = logging.getLogger(__name__)

processors = []  # global list to facilitate clean signal handling
exiting = False

LOG_FORMAT = '%(asctime)s %(levelname)-6s %(name)-12s %(message)s'
LOG_DATE_FORMAT = '%Y-%m-%d %H:%M:%S'


def clean_exit(signum, frame=None):
    """clean exit

       Exit all processes attempting to finish uncommited active
       work before exit. Can be called on an os signal or on zookeeper
       losing connection.
    """
    global exiting
    if exiting:
        # Since this is set up as a handler for SIGCHLD when this
        # kills one child it gets another signal, the global
        # exiting avoids this running multiple times.
        log.debug(
            'Exit in progress clean_exit received additional signal %s' %
            signum)
        return

    log.info('Received signal %s, beginning graceful shutdown.' % signum)
    exiting = True

    for process in processors:
        try:
            if process.is_alive():
                process.terminate()
        except Exception:
            pass

    # Kill everything, that didn't already die
    for child in multiprocessing.active_children():
        log.debug('Killing pid %s' % child.pid)
        try:
            os.kill(child.pid, signal.SIGKILL)
        except Exception:
            pass

    sys.exit(0)


def register_logging_opts(conf):
    logging_opts = [
        cfg.StrOpt('log_level', default='INFO'),
        cfg.StrOpt('log_file', default='./events_engine.log')]
    logging_group = cfg.OptGroup(name='logging', title='logging')

    conf.register_group(logging_group)
    conf.register_opts(logging_opts, logging_group)


def register_mysql_opts(conf):
    mysql_opts = [
        cfg.StrOpt('database_name'), cfg.StrOpt('hostname'),
        cfg.StrOpt('username'), cfg.StrOpt('password')]
    mysql_group = cfg.OptGroup(name='mysql', title='mysql')

    conf.register_group(mysql_group)
    conf.register_opts(mysql_opts, mysql_group)


def register_kafka_opts(conf):
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
                   help='The topic for reading transformed events.'),
        cfg.IntOpt(
            'events_fetch_size_bytes', default=32768,
            help='The number of fetch size bytes.'),
        cfg.IntOpt(
            'events_buffer_size', default=32768, help='The buffer size.'),
        cfg.IntOpt(
            'events_max_buffer_size', default=262144,
            help='Recommended 8 times the buffer size.')]

    kafka_group = cfg.OptGroup(name='kafka', title='title')
    conf.register_group(kafka_group)
    conf.register_opts(kafka_opts, kafka_group)


def register_winchester_opts(conf):
    winchester_opts = [
        cfg.StrOpt('winchester_config', default='../etc/winchester.yaml',
                   help='Path to the winchester.yaml file.')]
    winchester_group = cfg.OptGroup(name='winchester', title='title')
    conf.register_group(winchester_group)
    conf.register_opts(winchester_opts, winchester_group)


def register_event_processor_opts(conf):
    event_proc_opts = [
        cfg.IntOpt(
            'number', default=1,
            help='The number of processes to start.')]
    event_proc_group = cfg.OptGroup(name='event_processor', title='title')
    conf.register_group(event_proc_group)
    conf.register_opts(event_proc_opts, event_proc_group)


def register_pipeline_processor_opts(conf):
    pipeline_proc_opts = [
        cfg.IntOpt(
            'number', default=1,
            help='The number of processes to start.')]
    pipeline_proc_group = cfg.OptGroup(
        name='pipeline_processor', title='title')
    conf.register_group(pipeline_proc_group)
    conf.register_opts(pipeline_proc_opts, pipeline_proc_group)


def register_zookeeper_opts(conf):
    zookeeper_opts = [
        cfg.StrOpt('url', default='notification-events',
                   help='The topic for sending notification events.')]
    zookeeper_group = cfg.OptGroup(name='zookeeper', title='title')
    conf.register_group(zookeeper_group)
    conf.register_opts(zookeeper_opts, zookeeper_group)


def main(argv=None):
    if argv is None:
        argv = sys.argv
    if len(argv) == 2:
        config_file = argv[1]
    elif len(argv) > 2:
        print("Usage: " + argv[0] + " <config_file>")
        print(
            "Config file defaults to /etc/monasca/monasca_events_engine.conf")
        return 1
    else:
        config_file = '/etc/monasca/monasca_events_engine.conf'

    # init oslo config
    cfg.CONF(args=[],
             project='monasca_events_engine',
             default_config_files=[config_file])
    conf = cfg.CONF

    # register oslo config opts
    register_logging_opts(conf)
    register_mysql_opts(conf)
    register_kafka_opts(conf)
    register_winchester_opts(conf)
    register_event_processor_opts(conf)
    register_pipeline_processor_opts(conf)
    register_zookeeper_opts(conf)

    # Setup python logging
    log_file = conf.logging.log_file
    log_level = conf.logging.log_level
    fh = logging.handlers.RotatingFileHandler(
        log_file, maxBytes=10485760, backupCount=5)
    fh.setFormatter(logging.Formatter(fmt=LOG_FORMAT,
                                      datefmt=LOG_DATE_FORMAT))
    logging.getLogger('').addHandler(fh)
    ch = logging.StreamHandler()
    ch.setFormatter(logging.Formatter(fmt=LOG_FORMAT,
                                      datefmt=LOG_DATE_FORMAT))
    if log_level == "DEBUG":
        logging.getLogger('').setLevel(logging.DEBUG)
        ch.setLevel(logging.DEBUG)
    elif log_level == "INFO":
        logging.getLogger('').setLevel(logging.INFO)
        ch.setLevel(logging.INFO)
    elif log_level == "WARNING":
        logging.getLogger('').setLevel(logging.WARNING)
        ch.setLevel(logging.WARNING)
    elif log_level == "ERROR":
        logging.getLogger('').setLevel(logging.ERROR)
        ch.setLevel(logging.ERROR)
    else:
        logging.getLogger('').setLevel(logging.INFO)
        ch.setLevel(logging.INFO)
    logging.getLogger('').addHandler(ch)
    logging.getLogger('kafka').setLevel(logging.WARN)
    logging.getLogger('iso8601').setLevel(logging.WARN)

    # create EventProcessor(s)
    num_event_processors = conf.event_processor.number
    log.info('num_event_processors %d', num_event_processors)
    for x in xrange(0, num_event_processors):
        event_processor = multiprocessing.Process(
            target=EventProcessor(conf).run
        )
        processors.append(event_processor)

    # create PipelineProcessor(s)
    num_pipeline_processors = conf.pipeline_processor.number
    log.info('num_pipeline_processors %d', num_pipeline_processors)
    for x in xrange(0, num_pipeline_processors):
        pipeline_processor = multiprocessing.Process(
            target=PipelineProcessor(
                conf).run
        )
        processors.append(pipeline_processor)

    # Start
    try:
        log.info('Starting processes')
        for process in processors:
            process.start()

        # The signal handlers must be added after the processes start otherwise
        # they run on all processes
        signal.signal(signal.SIGCHLD, clean_exit)
        signal.signal(signal.SIGINT, clean_exit)
        signal.signal(signal.SIGTERM, clean_exit)

        while True:
            time.sleep(5)

    except Exception:
        log.exception('Error! Exiting.')
        for process in processors:
            process.terminate()

if __name__ == "__main__":
    sys.exit(main())
