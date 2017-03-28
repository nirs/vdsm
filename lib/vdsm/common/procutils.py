# Copyright 2017 Red Hat, Inc.
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

import errno
import io
import logging
import os
import select
import time

from vdsm import utils
from vdsm.common import errors
from vdsm.common import osutils

OUT = "out"
ERR = "err"

log = logging.getLogger("procutils")


class TimeoutExpired(errors.Base):
    msg = "Timeout waiting for process pid={self.pid}"

    def __init__(self, pid):
        self.pid = pid


def communicate(p, timeout=None, bufsize=io.DEFAULT_BUFFER_SIZE):
    """
    Communicate with process, yielding data read from stdout and stderr until
    proccess terminates or timeout expires.

    Unlike Popen.communicate, this support a timeout, and allows reading both
    stdout and stderr with a single thread.

    Reading data from both stdout and stderr:

        for src, data in procutils.communicate(proc):
            if src == procutils.OUT:
                # handle output
            elif src == procutils.ERR:
                # handler errors

    Waiting for process with a timeout:

        for _ in procutils.communicate(proc, 10):
            pass
    """
    if timeout is not None:
        deadline = utils.monotonic_time() + timeout
    else:
        deadline = None

    fds = {}
    if p.stdout:
        fds[p.stdout.fileno()] = OUT
    if p.stderr:
        fds[p.stderr.fileno()] = ERR

    if fds:
        poller = select.poll()
        for fd in fds:
            poller.register(fd, select.POLLIN)

        def discard(fd):
            if fd in fds:
                del fds[fd]
                poller.unregister(fd)

    while fds:
        log.debug("Waiting for process (pid=%d, timeout=%s)",
                  p.pid, timeout)
        if deadline:
            timeout = timeout * 1000
        try:
            ready = poller.poll(timeout)
        except select.error as e:
            if e[0] != errno.EINTR:
                raise
            log.debug("Polling process (pid=%d) interrupted", p.pid)
        else:
            for fd, mode in ready:
                if mode & select.POLLIN:
                    data = osutils.uninterruptible(os.read, fd, bufsize)
                    if not data:
                        log.debug("Fd %d closed, unregistering", fd)
                        discard(fd)
                        continue
                    yield fds[fd], data
                else:
                    log.debug("Fd %d hangup/error, unregistering", fd)
                    discard(fd)
        if deadline:
            timeout = deadline - utils.monotonic_time()
            if timeout <= 0:
                raise TimeoutExpired(p.pid)

    _wait(p, deadline)


def _wait(p, deadline=None):
    """
    Wait until process terminate or if deadline is specified,
    utils.monotonic_time() exceeeds deadline.
    """
    log.debug("Waiting for process (pid=%d)", p.pid)
    if deadline is None:
        p.wait()
    else:
        timeout = 1.0 / 2**8
        while p.poll() is None:
            remaining = deadline - utils.monotonic_time()
            if remaining <= 0:
                raise TimeoutExpired(p.pid)
            time.sleep(min(timeout, remaining))
            if timeout < 1.0:
                timeout *= 2
    log.debug("Process (pid=%d) terminated", p.pid)
