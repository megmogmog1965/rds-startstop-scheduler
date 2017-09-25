#!/usr/bin/env python
# -*- coding: utf-8 -*-

'''
Created on Aug 29, 2017

:author: Yusuke Kawatsu
:require: pip install boto3 python-daemon
'''

# built-in modules.
import os
import sys
import time
import argparse
import datetime
import threading
import traceback

# installed modules.
import boto3
import daemon


# path constants.
_FILENAME = os.path.basename(__file__)
_FILEDIR = os.path.dirname(os.path.abspath(__file__))

# launch daemon.
context = daemon.DaemonContext(
    working_directory = _FILEDIR,
    stdout = open(u'%s.stdout' % (_FILENAME), 'a+'),  # stdout
    stderr = open(u'%s.stderr' % (_FILENAME), 'a+')   # stderr
)

# disable buffering.
# :see: http://stackoverflow.com/questions/881696/unbuffered-stdout-in-python-as-in-python-u-from-within-the-program
sys.stdout = os.fdopen(sys.stdout.fileno(), 'w', 0)
sys.stderr = os.fdopen(sys.stderr.fileno(), 'w', 0)

# command line args.
_parser = argparse.ArgumentParser(description=u'auto start/stop RDS by "start_time" tag and "stop_time" tag.')
_parser.add_argument(u'--aws-access-key-id', help=u'aws access key id.')
_parser.add_argument(u'--aws-secret-access-key', help=u'aws secret access key.')
_parser.add_argument(u'--region', default=u'us-east-1', help=u'AWS region name.')
_parser.add_argument(u'--profile', help=u'AWS credential profile name')
_parser.add_argument(u'--list', action='store_true', help=u'List all scheduled actions.')
_args = _parser.parse_args()


def daemon_process():
    '''
    entrypoint for running as daemon.
    '''
    # worker thread for RDS instances that have "stop_time" tag.
    threading.Thread(target=lambda: _rds_action_loop(_rds_generator_with_timetag(u'stop_time'), _shutdown_instance)).start()

    # worker thread for RDS instances that have "start_time" tag.
    threading.Thread(target=lambda: _rds_action_loop(_rds_generator_with_timetag(u'start_time'), _start_instance)).start()

    while True:
        time.sleep(60 * 60)

def _rds_action_loop(rds_generator, action):
    '''
    thread action.

    :param rds_generator: a generator of (datetime, rds_object).
    :param action: (rds_object):void -> {...}
    '''
    for tm, ins in rds_generator:
        # wait until the scheduled time comes.
        while True:
            # end main threadsa
            main_thread = filter(lambda th: isinstance(th, threading._MainThread), threading.enumerate())
            if not main_thread or not main_thread.pop().is_alive():
                return
            
            # sleep 10 secs.
            time.sleep(10)
            
            # guard condition.
            now = datetime.datetime.now()
            if now > tm:
                break

        # call action.
        _try(lambda: action(ins))()

def _aws_client():
    '''
    :rtype: :class:`boto3.client`
    '''
    opt_params = {}
    opt_params = dict(opt_params, profile_name=_args.profile) if _args.profile else opt_params
    session = boto3.Session(**opt_params)

    opt_params = {}
    opt_params = dict(opt_params, aws_access_key_id=_args.aws_access_key_id) if _args.aws_access_key_id else opt_params
    opt_params = dict(opt_params, aws_secret_access_key=_args.aws_secret_access_key) if _args.aws_secret_access_key else opt_params
    opt_params = dict(opt_params, region_name=_args.region) if _args.region else opt_params

    return session.client('rds', **opt_params)

def _shutdown_instance(ins):
    ''' stop RDS instance '''
    _print_instance_state_changes([ins], u'stopped')
    client = _aws_client()
    client.stop_db_instance(DBInstanceIdentifier=ins[u'DBInstanceIdentifier'])

def _start_instance(ins):
    ''' start RDS instance '''
    _print_instance_state_changes([ins], u'started')
    client = _aws_client()
    client.start_db_instance(DBInstanceIdentifier=ins[u'DBInstanceIdentifier'])

__retry=50
def _try(closure):
    def _f():
        global __retry
        try:
            # ignore errors 50 times.
            return closure()
        
        except:
            # error logging.
            message = traceback.format_exc()
            sys.stderr.write(message + '\n')

            # ignore errors 50 times.
            if __retry <= 0:
                sys.exit(1)
        __retry = __retry - 1
    
    return _f

def _rds_generator_with_timetag(timetag):
    '''
    :return: an infinite generator of rds_object.
    '''
    rds_list = []
    while True:
        if not rds_list:
            rds_list = _try(lambda: _ordered_rds_list(timetag))()
            rds_list = rds_list if rds_list else []
            rds_list.reverse()
        
        if not rds_list:
            time.sleep(60 * 60) # wait 1 hour.
            continue
        
        yield rds_list.pop()

def _ordered_rds_list(timetag):
    '''
    :param timetag: time tag name.
    :return: same as __find_all_rds() but ordered.
    '''
    mid = _find_all_rds()
    mid = filter(lambda ins: timetag in ins[u'Tag'], mid)  # filter instances that have "shutdown_time" tag.
    mid = map(lambda ins: [[tm, ins] for tm in _parse_timetag(ins, timetag)], mid)  # rds_object to [ datetime.time, rds_object ].
    mid = reduce(lambda pre, cur: pre + cur, mid, [])  # flatten.
    mid = filter(lambda pair: pair[0], mid)  # filter instances that have valid date format.
    mid = sorted(mid, key=lambda pair: pair[0])  # sorted by date.

    # sorted by current datetime.
    now = datetime.datetime.now()
    past = filter(lambda pair: pair[0] < now, mid)
    nextday = map(lambda pair: [pair[0] + datetime.timedelta(days=1), pair[1]], past)  # make past next day.
    today = filter(lambda pair: pair[0] > now, mid)
    ordered = today + nextday

    return ordered

def _parse_timetag(rds_object, timetag):
    '''
    :param dict rds_object: rds_object
    :param unicode timetag: like "08:30".
    :rtype: :class:`datetime.datetime`
    :rtype: None
    '''
    tag = rds_object[u'Tag'][timetag]
    time_strs = tag.split(u',')

    def _str_to_time(time_str):
        for time_format in [u'%H:%M', u'%H:%M:%S']:
            try:
                time = datetime.datetime.strptime(time_str, time_format)
                now = datetime.datetime.now()
                return datetime.datetime(now.year, now.month, now.day, time.hour, time.minute, time.second)
            except:
                pass
        return None # failed to parse.

    ret = map(lambda t: _str_to_time(t), time_strs)

    return ret

def _find_all_rds(state=None):
    '''
    :rtype: list of dict.
    :return: a list of rds_object.
    '''
    # access AWS console.
    client = _aws_client()

    # collect.
    instances = client.describe_db_instances()[u'DBInstances']

    # filter by state.
    instances = filter(lambda ins: ins[u'DBInstanceStatus'] == state if state else True, instances)

    # add tags.
    arns = map(lambda ins: ins[u'DBInstanceArn'], instances)
    tags = map(lambda arn: client.list_tags_for_resource(ResourceName=arn)[u'TagList'], arns)
    tags = map(lambda tag: map(lambda kv: (kv[u'Key'],kv[u'Value'],), tag), tags)
    tags = map(lambda tag: dict((key, val) for key, val in tag), tags)
    instances = map(lambda (ins, tag): dict(ins, Tag=tag), zip(instances, tags))
    
    return instances

def _print_instance_state_changes(instances, new_state_message):
    '''
    print new instance state.
    '''
    for instance in instances:
        details = { u'DBInstanceIdentifier': instance[u'DBInstanceIdentifier'] }
        print u'At %s, %s instance: %s' % (datetime.datetime.now().isoformat(), new_state_message, details)

def _list_scheduled_actions(rds_list):
    ''' for "--list" option '''
    filtered = filter(lambda rds: u'start_time' in rds[u'Tag'] or u'stop_time' in rds[u'Tag'], rds_list)
    out = reduce(lambda pre,rds: u'%s\n%20s : %s' % (pre, rds[u'DBInstanceIdentifier'], rds[u'Tag'],), filtered, u'')
    print out


if __name__ == '__main__':
    # "--list".
    _list_scheduled_actions(_find_all_rds())
    if _args.list:
        sys.exit(0)

    # launch daemon.
    with context:
        daemon_process()
