#!/usr/bin/env python
#
# Copyright 2011 Facebook
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

"""Utilities for working with multiple processes."""

import errno
import logging
import os
import sys
import time

from binascii import hexlify

from tornado import ioloop

try:
    import multiprocessing # Python 2.6+
except ImportError:
    multiprocessing = None

try:
    import psutil
except ImportError:
    psutil = None

# probably can just import, but better to be safe
try:
    import signal
except ImportError:
    signal = None

def cpu_count():
    """Returns the number of processors on this machine."""
    if multiprocessing is not None:
        try:
            return multiprocessing.cpu_count()
        except NotImplementedError:
            pass
    try:
        return os.sysconf("SC_NPROCESSORS_CONF")
    except ValueError:
        pass
    logging.error("Could not detect number of processors; assuming 1")
    return 1

def _reseed_random():
    if 'random' not in sys.modules:
        return
    import random
    # If os.urandom is available, this method does the same thing as
    # random.seed (at least as of python 2.6).  If os.urandom is not
    # available, we mix in the pid in addition to a timestamp.
    try:
        seed = long(hexlify(os.urandom(16)), 16)
    except NotImplementedError:
        seed(int(time.time() * 1000) ^ os.getpid())
    random.seed(seed)


_task_id = None

def fork_processes(num_processes, max_restarts=100, child_pids=None):
    """Starts multiple worker processes.

    If ``num_processes`` is None or <= 0, we detect the number of cores
    available on this machine and fork that number of child
    processes. If ``num_processes`` is given and > 0, we fork that
    specific number of sub-processes.

    Since we use processes and not threads, there is no shared memory
    between any server code.

    Note that multiple processes are not compatible with the autoreload
    module (or the debug=True option to `tornado.web.Application`).
    When using multiple processes, no IOLoops can be created or
    referenced until after the call to ``fork_processes``.

    In each child process, ``fork_processes`` returns its *task id*, a
    number between 0 and ``num_processes``.  Processes that exit
    abnormally (due to a signal or non-zero exit status) are restarted
    with the same id (up to ``max_restarts`` times).  In the parent
    process, ``fork_processes`` returns None if all child processes
    have exited normally, but will otherwise only exit by throwing an
    exception.
    """
    global _task_id
    assert _task_id is None
    if num_processes is None or num_processes <= 0:
        num_processes = cpu_count()
    if ioloop.IOLoop.initialized():
        raise RuntimeError("Cannot run in multiple processes: IOLoop instance "
                           "has already been initialized. You cannot call "
                           "IOLoop.instance() before calling start_processes()")
    logging.info("Starting %d processes", num_processes)
    children = {}
    def start_child(i):
        pid = os.fork()
        if pid == 0:
            # child process
            _reseed_random()
            global _task_id
            _task_id = i
            return i
        else:
            # NOTE [adam Nov/11/12]: bit of a hack... lists behave as
            #      pass-by-reference, so this lets me minimize restructuring
            #      of this module and still get a list of child pids into
            #      the application object
            if child_pids is not None:
                child_pids.append(pid)

            children[pid] = i
            return None
    for i in range(num_processes):
        id = start_child(i)
        if id is not None: return id
    num_restarts = 0
    while children:
        try:
            pid, status = os.wait()
        except OSError, e:
            if e.errno == errno.EINTR:
                continue
            raise
        if pid not in children:
            continue
        id = children.pop(pid)
        if os.WIFSIGNALED(status):
            logging.warning("child %d (pid %d) killed by signal %d, restarting",
                            id, pid, os.WTERMSIG(status))
        elif os.WEXITSTATUS(status) != 0:
            logging.warning("child %d (pid %d) exited with status %d, restarting",
                            id, pid, os.WEXITSTATUS(status))
        else:
            logging.info("child %d (pid %d) exited normally", id, pid)
            continue
        num_restarts += 1
        if num_restarts > max_restarts:
            raise RuntimeError("Too many child restarts, giving up")
        new_id = start_child(id)
        if new_id is not None: return new_id

STOPLOSS_RATIO    = 0.9 # can't terminate more than this ratio of total
                        # processes in a STOPLOSS_PERIOD
TIMEOUT_PERIOD    = 60  # period in seconds after which a process is dead
SLEEP_PERIOD      = 30  # period in seconds between watchdog checks
                        # also give this much time for a SIGTERM'd process
                        # to gracefully shutdown
GRACE_PERIOD      = 300 # period in seconds to allow a new tornado to spin up
MAX_RSS           = 1536 * 1024 * 1024 # Allow up to 1.5G of memory per process

def fork_processes_with_watchdog(
    num_processes, is_shutdown_callback, child_pids=None,
    stoploss_ratio=STOPLOSS_RATIO, timeout_period=TIMEOUT_PERIOD,
    sleep_period=SLEEP_PERIOD, grace_period=GRACE_PERIOD,
    max_rss=MAX_RSS, statsd=None, app_name='default'):
    """Starts multiple worker processes.

    If ``num_processes`` is None or <= 0, we detect the number of cores
    available on this machine and fork that number of child
    processes. If ``num_processes`` is given and > 0, we fork that
    specific number of sub-processes.

    Since we use processes and not threads, there is no shared memory
    between any server code.

    Note that multiple processes are not compatible with the autoreload
    module (or the debug=True option to `tornado.web.Application`).
    When using multiple processes, no IOLoops can be created or
    referenced until after the call to ``fork_processes``.

    In each child process, ``fork_processes`` returns its *task id*, a
    number between 0 and ``num_processes``.  Processes that exit
    abnormally (due to a signal or non-zero exit status) are restarted
    with the same id (up to ``max_restarts`` times).  In the parent
    process, ``fork_processes`` returns None if all child processes
    have exited normally, but will otherwise only exit by throwing an
    exception.
    """
    global _task_id
    assert _task_id is None
    assert multiprocessing is not None
    assert psutil is not None

    if num_processes is None or num_processes <= 0:
        num_processes = cpu_count()
    if ioloop.IOLoop.initialized():
        raise RuntimeError("Cannot run in multiple processes: IOLoop instance "
                           "has already been initialized. You cannot call "
                           "IOLoop.instance() before calling start_processes()")
    logging.info("Starting %d processes", num_processes)
    children = {}
    pipes = {}
    last_checkin = {}
    processes = {}
    healthy = set()

    def send_stat(metric, val):
        if statsd:
            statsd.incr(metric,val,use_prefix=False)

    def start_child(i):
        parent_conn, child_conn = multiprocessing.Pipe(duplex=False)
        pid = os.fork()
        if pid == 0:
            # child process
            _reseed_random()
            global _task_id
            _task_id = i
            return i, child_conn
        else:
            # NOTE [adam Nov/11/12]: bit of a hack... lists behave as
            #      pass-by-reference, so this lets me minimize restructuring
            #      of this module and still get a list of child pids into
            #      the application object
            if child_pids is not None:
                child_pids.append(pid)

            children[pid] = i
            last_checkin[pid] = time.time() + grace_period
            pipes[pid] = (parent_conn, child_conn)
            processes[pid] = psutil.Process(pid)
            logging.info("Started new child process with number %d pid %d",
                i, pid)
            return None, None

    def cleanup_pid(pid, remove_from_child_pids=True):
        if remove_from_child_pids and pid in child_pids:
            child_pids.remove(pid)
        if pid in pipes:
            parent_pipe, child_pipe = pipes[pid]
            try:
                parent_pipe.close()
            except Exception:
                logging.exception("Problem while closing pipe for "\
                                  "pid %d",
                                  pid)
            try:
                child_pipe.close()
            except Exception:
                logging.exception("Problem while closing pipe for "\
                                  "pid %d",
                                  pid)
        pipes.pop(pid, None)
        last_checkin.pop(pid, None)
        children.pop(pid, None)
        processes.pop(pid, None)
        if pid in healthy:
            healthy.remove(pid)

    def stoplossed():
        if float(len(healthy)) / num_processes < stoploss_ratio:
            return True
        return False

    for i in range(num_processes):
        _id, child_conn = start_child(i)
        if _id is not None: return _id, child_conn

    # for keeping track of which processes we've gracefully killed
    # due to memory issues
    sent_term = set()
    while children:
        try:
            # Try to respawn any children that don't exist yet
            # this can happen during bad site issues where a process
            # dies because it can't connect to mongo, etc and then gets
            # reaped by our upstart checker. This will put us in a period
            # of permanent stoploss, so to avoid that we respawn here
            # Only execute if we're not shutting down.
            if not is_shutdown_callback():
                alive_children = set(children.values())
                children_to_spawn = set(range(num_processes)) - alive_children
                for child_number in children_to_spawn:
                    _id, child_conn = start_child(child_number)
                    if _id is not None: return _id, child_conn
            else:
                # there's a race where we can spawn children after the tornado
                # app has received the shutdown signal. This will ensure we will
                # eventually shutdown. Should happen very rarely
                for pid, child_number in children.iteritems():
                    try:
                        os.kill(pid, signal.SIGTERM)
                        if pid in healthy:
                            healthy.remove(pid)
                        send_stat('infra.frontend.%s.shutdown_term' % app_name,1)
                    except OSError:
                        logging.exception(
                            "Failed to terminate pid %d process number %d "\
                                "while shutting down",
                            pid, child_number)

            # for all the processes we've sent SIGTERM, make sure they are
            # not running anymore - if they are escalate to SIGKILL.
            # Shouldn't need to check for stoploss, since len(sent_term)
            # must be < what would put us over the stoploss
            # We need to do this first so that we don't mistakenly think that
            # upstart is trying to shut us down since that also sends a SIGTERM
            # to the child processes
            for i, (pid, child_number) in enumerate(sent_term):

                if pid not in processes:
                    continue

                if processes[pid].is_running():
                    logging.info("Trying to kill pid %d process number %d",
                                 pid, child_number)
                    try:
                        os.kill(pid, signal.SIGKILL)
                        send_stat('infra.frontend.%s.kill' % app_name,1)
                    except OSError:
                        logging.exception("Failed to kill pid %d process number %d",
                            pid, child_number)

                    logging.info("Trying to wait on pid %d process number %d",
                                 pid, child_number)
                    try:
                        os.waitpid(pid, os.WNOHANG)
                    except OSError:
                        logging.exception("Failed to wait on pid %d process number %d",
                                     pid, child_number)

                cleanup_pid(pid)

                if not is_shutdown_callback():
                    _id, child_conn = start_child(child_number)
                    if _id is not None: return _id, child_conn

            # reset this
            sent_term = set()
            # Detect if we're being shut down by upstart, in which case
            # we no longer want to respawn child processes that have died
            # also detect if the child has exited on its own (died on startup)
            to_cleanup = set()
            for pid, _ in children.iteritems():
                try:
                    _resp_pid, status = os.waitpid(pid, os.WNOHANG)
                except OSError as e:
                    if e.errno == errno.EINTR:
                        continue
                    if e.errno != errno.ECHILD:
                        raise
                # Still alive
                if status == 0:
                    continue
                to_cleanup.add(pid)
                logging.info("Detected that pid %d died", pid)

            for pid in to_cleanup:
                cleanup_pid(pid, remove_from_child_pids=False)

            # If we have child processes, see if they're up and running
            to_reap = set()
            exceed_mem = set()
            for pid, child_number in children.iteritems():
                parent_pipe, child_pipe = pipes[pid]

                got_msg = False
                # get the last message that was sent
                try:
                    while parent_pipe.poll():
                        status = parent_pipe.recv()
                        got_msg = True
                except Exception:
                    logging.exception("Problem while polling pipe for "\
                                      "pid %d process number %d",
                                      pid, child_number)

                if got_msg:
                    if not status:
                        logging.info(
                            "Empty status message from pid %d process number %d",
                            pid, child_number)
                    if 'checkin_time' not in status:
                        logging.info(
                            "Check in time not reported from pid %d process number %d",
                            pid, child_number)
                        last_checkin[pid] = time.time()
                        healthy.add(pid)
                    else:
                        last_checkin[pid] = status['checkin_time']
                        healthy.add(pid)

                if time.time() - last_checkin[pid] > timeout_period:
                    to_reap.add((pid, child_number))
                    logging.info(
                        "Scheduling pid %d process number %d to be reaped "\
                            'for failing to check in after %d seconds',
                        pid, child_number, timeout_period)

                try:
                    # only terminate if it's after grace period
                    if processes[pid].create_time() + grace_period < time.time():
                        rss, _ = processes[pid].memory_info()[:2]
                        if rss > max_rss:
                            exceed_mem.add((pid, child_number))
                            logging.info(
                                "Scheduling pid %d process number %d to be reaped "\
                                    'for exceeding MAX_RSS (%dMB/%dMB)',
                                pid, child_number,
                                rss / (1024 * 1024), max_rss / (1024 * 1024))
                except Exception:
                    logging.exception(
                        "Unable to get RSS from pid %d process number %d",
                        pid, child_number)

            # Reap the child processes that are stuck
            for i, (pid, child_number) in enumerate(to_reap):
                if stoplossed():
                    logging.info(
                        "Not enough tornadoes healthy, stoploss initiated.")
                    send_stat('infra.frontend.%s.stoplossed' % app_name,1)
                    break

                logging.info("Trying to gracefully terminate pid "\
                             "%d process number %d",
                    pid, child_number)
                try:
                    os.kill(pid, signal.SIGTERM)
                    sent_term.add((pid, child_number))
                    if pid in healthy:
                        healthy.remove(pid)
                    send_stat('infra.frontend.%s.term' % app_name,1)
                except OSError:
                    logging.exception("Failed to terminate pid %d process number %d",
                        pid, child_number)

            # if its timed out, we've already termed it
            exceed_mem -= to_reap

            for i, (pid, child_number) in enumerate(exceed_mem):
                if stoplossed():
                    logging.info(
                        "Not enough tornadoes healthy, stoploss initiated.")
                    send_stat('infra.frontend.%s.stoplossed_mem' % app_name,1)
                    break

                logging.info("Trying to gracefully terminate pid %d process number %d",
                    pid, child_number)
                try:
                    os.kill(pid, signal.SIGTERM)
                    sent_term.add((pid, child_number))
                    if pid in healthy:
                        healthy.remove(pid)
                    send_stat('infra.frontend.%s.term_mem' % app_name,1)
                except OSError:
                    logging.exception("Failed to terminate pid %d process number %d",
                        pid, child_number)

            time.sleep(sleep_period)
        except Exception:
            logging.exception("Unhandled error in watchdog loop")
            send_stat('infra.frontend.%s.unhandled_exception' % app_name,1)
    return None, None

def task_id():
    """Returns the current task id, if any.

    Returns None if this process was not created by `fork_processes`.
    """
    global _task_id
    return _task_id
