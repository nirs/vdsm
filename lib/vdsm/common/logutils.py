#
# Copyright 2011-2017 Red Hat, Inc.
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation; either version 2 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program; if not, write to the Free Software
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA 02110-1301 USA
#
# Refer to the README and COPYING files for full details of the license
#
from __future__ import absolute_import

import collections
import datetime
import functools
import grp
import logging
import logging.handlers
import os
import pwd
import threading

from dateutil import tz
from inspect import ismethod

import six

from . import concurrent


def funcName(func):
    if ismethod(func):
        return func.__func__.__name__

    if hasattr(func, 'func'):
        return func.func.__name__

    return func.__name__


def call2str(func, args, kwargs, printers={}):
    kwargs = kwargs.copy()
    varnames = func.__code__.co_varnames[:func.__code__.co_argcount]
    if ismethod(func):
        args = [func.__self__] + list(args)
        func = func.__func__

    for name, val in zip(varnames, args):
        kwargs[name] = val

    defaults = func.__defaults__ if func.__defaults__ else []

    for name, val in zip(varnames[-len(defaults):], defaults):
        if name not in kwargs:
            kwargs[name] = val

    argsStrs = []
    for i, argName in enumerate(varnames):
        if i == 0 and argName == "self":
            continue

        val = kwargs[argName]
        printer = printers.get(argName, repr)
        argsStrs.append("%s=%s" % (argName, printer(val)))

    return "%s(%s)" % (func.__name__, ", ".join(argsStrs))


class SimpleLogAdapter(logging.LoggerAdapter):
    # Because of how python implements the fact that warning
    # and warn are the same. I need to reimplement it here. :(
    warn = logging.LoggerAdapter.warning

    def __init__(self, logger, context):
        """
        Initialize an adapter with a logger and a dict-like object which
        provides contextual information. The contextual information is
        prepended to each log message.

        This adapter::

            self.log = SimpleLogAdapter(self.log, {"task": "xxxyyy",
                                                   "res", "foo.bar.baz"})
            self.log.debug("Message")

        Would produce this message::

            "(task='xxxyyy', res='foo.bar.baz') Message"
        """
        self.logger = logger
        items = ", ".join(
            "%s='%s'" % (k, v) for k, v in six.viewitems(context))
        self.prefix = "(%s) " % items

    def process(self, msg, kwargs):
        return self.prefix + msg, kwargs


class UserGroupEnforcingHandler(logging.handlers.WatchedFileHandler):
    """
    This log handler acts like WatchedFileHandler.
    Additionally, upon file access, handler check the credentials of running
    process,to make sure log is not created with wrong permissions by mistake.
    """

    def __init__(self, user, group, *args, **kwargs):
        self._uid = pwd.getpwnam(user).pw_uid
        self._gid = grp.getgrnam(group).gr_gid
        logging.handlers.WatchedFileHandler.__init__(self, *args, **kwargs)

        # To trigger cred check:
        self._open()

    def _open(self):
        if (os.geteuid() != self._uid) or (os.getegid() != self._gid):
            raise RuntimeError(
                "Attempt to open log with incorrect credentials")
        return logging.handlers.WatchedFileHandler._open(self)


class TimezoneFormatter(logging.Formatter):
    def converter(self, timestamp):
        return datetime.datetime.fromtimestamp(timestamp,
                                               tz.tzlocal())

    def formatTime(self, record, datefmt=None):
        ct = self.converter(record.created)
        if datefmt:
            s = ct.strftime(datefmt, ct)
        else:
            s = "%s,%03d%s" % (
                ct.strftime('%Y-%m-%d %H:%M:%S'),
                record.msecs,
                ct.strftime('%z')
            )
        return s


class ThreadedHandler(logging.Handler):
    """
    A handler queuing records and logging them in a background thread using
    UserGroupEnforcingHandler.
    """

    _CLOSED = object()

    def __init__(self, user, group, filename, queue_size=10000):
        """
        Arguments:
            user (str): logfile user name.
            group (str): logfile group name.
            filename (str): log filename
            queue_size (int): number of records to queue before dropping
                records. When the queue becomes full, oldest records are
                dropped.  Using default value to keep __init__ signature same
                as UserGroupEnforcingHandler, so configuring this handler
                requires only a class name change.
        """
        logging.Handler.__init__(self)
        self._handler = UserGroupEnforcingHandler(user, group, filename)
        self._queue = collections.deque(maxlen=queue_size)
        self._cond = threading.Condition(threading.Lock())
        self._thread = concurrent.thread(self._run, name="logfile")
        self._thread.start()

    # Handler interface

    def createLock(self):
        """
        Override to avoid unneeded lock. We use a condition to synchronize with
        the logging thread.
        """
        self.lock = None

    def handle(self, record):
        """
        Handle a log record.

        If the queue is full, oldest record is dropped to make room for the new
        record.
        """
        with self._cond:
            self._queue.append(record)
            self._cond.notify()

    def setFormatter(self, fmt):
        """
        Override to pass the formatter to the underlying handler.
        """
        self._handler.setFormatter(fmt)

    def close(self):
        """
        Extend Handler.close to stop the thread during shutdown.
        """
        logging.Handler.close(self)
        with self._cond:
            self._queue.append(self._CLOSED)
            self._cond.notify()
        self._thread.join()

    # Private

    def _run(self):
        while True:
            # Wait for messages.
            with self._cond:
                while len(self._queue) == 0:
                    self._cond.wait()

            # Handle all pending messages before taking the lock again.
            pending = len(self._queue)
            for _ in range(pending):
                record = self._queue.popleft()
                if record is self._CLOSED:
                    return
                self._handler.handle(record)

            # Avoid reference cycles, specially exc_info that may hold a
            # traceback objects.
            record = None


class Suppressed(object):

    def __init__(self, value):
        self._value = value

    @property
    def value(self):
        return self._value

    def __repr__(self):
        return '(suppressed)'


class AllVmStatsValue(Suppressed):

    def __repr__(self):
        return repr({vm.get('vmId'): vm.get('status') for vm in self._value})


def set_level(level_name, name=''):
    log_level = logging.getLevelName(level_name)
    if not isinstance(log_level, type(logging.DEBUG)):
        raise ValueError("unknown log level: %r" % level_name)

    log_name = None if not name else name
    # getLogger() default argument is None, not ''
    logger = logging.getLogger(log_name)
    logging.warning('Setting loglevel on %r to %s (%d)',
                    logger.name, level_name, log_level)
    logger.setLevel(log_level)


def volume_chain_to_str(base_first_chain):
    """
    Converts an iterable of volume UUIDs into a standard loggable
    format.  The first UUID should be the base (or oldest ancestor) and
    each subsequent entry a direct descendant of its predecessor.
    """
    return ' < '.join(base_first_chain) + " (top)"


def traceback(log=None, msg="Unhandled exception"):
    """
    Log a traceback for unhandled execptions.

    :param log: Use specific logger instead of root logger
    :type log: `logging.Logger`
    :param msg: Use specified message for the exception
    :type msg: str
    """
    def decorator(f):
        @functools.wraps(f)
        def wrapper(*a, **kw):
            try:
                return f(*a, **kw)
            except Exception:
                logger = log or logging.getLogger()
                logger.exception(msg)
                raise  # Do not swallow
        return wrapper
    return decorator
