#
# Copyright 2019 Red Hat, Inc.
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
from __future__ import division

from contextlib import contextmanager

from vdsm.storage import threadPool

from . storagetestlib import Callable


@contextmanager
def thread_pool(workers, wait_timeout=0.05):
    tp = threadPool.ThreadPool("name", workers, waitTimeout=wait_timeout)
    yield tp
    # TODO: Way to abort running tasks left by broken tests would be useful.
    tp.joinAll(waitForThreads=True)


def test_empty():
    with thread_pool(1):
        # No interesting behaviour to test, just ensure that stopping empty
        # pool does not fail.
        pass


def test_queue_task():
    with thread_pool(1) as tp:
        c = Callable(hang=False)
        tp.queueTask("id", c)
        # Raises Timeout if not called.
        c.wait_until_running(timeout=1)


def test_queue_task_with_args():
    with thread_pool(1) as tp:
        args = (1, 2)
        c = Callable(hang=False)
        tp.queueTask("id", c, args=args)
        # Raises Timeout if not called.
        c.wait_until_running(timeout=1)
        assert c.args == args


def test_queue_many_tasks():
    running = []
    queued = []
    workers = 10
    with thread_pool(workers) as tp:
        try:
            # Thesed tasks should run.
            for i in range(workers):
                c = Callable(hang=True)
                tp.queueTask("running{}".format(i), c)
                running.append(c)

            # These tasks should be queued.
            for i in range(workers):
                c = Callable(hang=True)
                tp.queueTask("queued{}".format(i), c)
                queued.append(c)

            for c in running:
                c.wait_until_running(timeout=1)

            for c in queued:
                assert not c.was_called()

            # Finish running tasks.
            for c in running:
                c.finish()

            # Queue tasks should run now.
            for c in queued:
                c.wait_until_running(1)
        finally:
            for c in running + queued:
                c.finish()


def test_failing_task():
    with thread_pool(1) as tp:
        tasks = []

        # These tasks will fail.
        for i in range(2):
            c = Callable(result=RuntimeError("no task for you!"))
            tp.queueTask("failure{}".format(i), c)
            tasks.append(c)

        # These tasks should succeed.
        for i in range(2):
            c = Callable()
            tasks.append(c)
            tp.queueTask("success{}".format(i), c)

        for c in tasks:
            c.wait_until_running(1)
