#
# Copyright 2016-2017 Red Hat, Inc.
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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301 USA
#
# Refer to the README and COPYING files for full details of the license
#

import grp
import logging
import os
import pwd

from contextlib import closing
from contextlib import contextmanager

from testlib import VdsmTestCase as TestCaseBase
from testlib import forked
from testlib import namedTemporaryDir

from vdsm.common import concurrent
from vdsm.common import logutils


class TestAllVmStats(TestCaseBase):

    _STATS = [{'foo': 'bar',
               'status': 'Up',
               'vmId': u'43f02a2d-e563-4f11-a7bc-9ee191cfeba1'},
              {'foo': 'bar',
               'status': 'Powering up',
               'vmId': u'bd0d066b-971e-42f8-8bc6-d647ab7e0e70'}]
    _SIMPLIFIED = ({u'43f02a2d-e563-4f11-a7bc-9ee191cfeba1': 'Up',
                    u'bd0d066b-971e-42f8-8bc6-d647ab7e0e70': 'Powering up'})

    def test_allvmstats(self):
        data = logutils.AllVmStatsValue(self._STATS)
        result = str(data)
        self.assertEqual(eval(result), self._SIMPLIFIED)


class TestSetLevel(TestCaseBase):

    @forked
    def test_root_logger(self):
        logger = logging.getLogger()
        logutils.set_level("WARNING")
        self.assertEqual(logger.getEffectiveLevel(), logging.WARNING)

    @forked
    def test_other_logger(self):
        name = "test"
        logger = logging.getLogger(name)
        logutils.set_level("WARNING", name=name)
        self.assertEqual(logger.getEffectiveLevel(), logging.WARNING)

    @forked
    def test_sub_logger(self):
        name = "test.sublogger"
        logger = logging.getLogger(name)
        logutils.set_level("WARNING", name=name)
        self.assertEqual(logger.getEffectiveLevel(), logging.WARNING)

    @forked
    def test_non_existing_level(self):
        with self.assertRaises(ValueError):
            logutils.set_level("NO SUCH LEVEL")

    @forked
    def test_level_alias(self):
        logging.addLevelName("OOPS", logging.ERROR)
        logger = logging.getLogger()

        # The new alias should work...
        logutils.set_level("OOPS")
        self.assertEqual(logger.getEffectiveLevel(), logging.ERROR)

        # The old name should work as well.
        logutils.set_level("ERROR")
        self.assertEqual(logger.getEffectiveLevel(), logging.ERROR)


@contextmanager
def threaded_handler(filename, queue_size):
    user = pwd.getpwuid(os.geteuid()).pw_name
    group = grp.getgrgid(os.getegid()).gr_name
    handler = logutils.ThreadedHandler(
        user, group, filename, queue_size=queue_size)
    with closing(handler):
        logger = logging.Logger("test")
        logger.addHandler(handler)
        yield handler, logger


class TestThreadedHandler(TestCaseBase):

    def test_queue_size(self):
        with namedTemporaryDir() as tmpdir:
            filename = os.path.join(tmpdir, "test.log")
            # Note: stopping the handler submits a sentinel record. If the
            # queue is full, this will drop the oldest record. Use 101 to
            # ensure that we log 100 messages.
            with threaded_handler(filename, 101) as (handler, logger):
                for _ in range(100):
                    logger.info("It works!")
            with open(filename) as f:
                lines = f.readlines()

        self.assertEqual(lines, ["It works!\n"] * 100)

    def test_drop_old_messages(self):
        with namedTemporaryDir() as tmpdir:
            filename = os.path.join(tmpdir, "test.log")
            with threaded_handler(filename, 10) as (handler, logger):
                for i in range(100):
                    logger.info("Message %d", i)
            with open(filename) as f:
                lines = f.readlines()

        # Recent message should be kept, older messages may have dropped.
        recent_messages = ["Message %d\n" % i for i in range(91, 100)]
        self.assertEqual(lines[-9:], recent_messages)

    def test_level_debug(self):
        with namedTemporaryDir() as tmpdir:
            filename = os.path.join(tmpdir, "test.log")
            with threaded_handler(filename, 10) as (handler, logger):
                handler.setLevel(logging.DEBUG)
                logger.debug("Should be logged")
            with open(filename) as f:
                lines = f.readlines()

        self.assertEqual("Should be logged\n", lines[0])

    def test_level_info(self):
        with namedTemporaryDir() as tmpdir:
            filename = os.path.join(tmpdir, "test.log")
            with threaded_handler(filename, 10) as (handler, logger):
                handler.setLevel(logging.INFO)
                logger.debug("Should not be logged")
            with open(filename) as f:
                lines = f.readlines()

        self.assertEqual(lines, [])

    def test_multiple_threads(self):
        with namedTemporaryDir() as tmpdir:
            filename = os.path.join(tmpdir, "test.log")
            with threaded_handler(filename, 101) as (handler, logger):

                def worker(n):
                    for i in range(10):
                        logger.info("Message %02d:%02d", n, i)

                threads = []
                for i in range(10):
                    t = concurrent.thread(worker, args=(i,))
                    t.start()
                    threads.append(t)

                for t in threads:
                    t.join()

            with open(filename) as f:
                lines = f.readlines()

        self.assertEqual(len(lines), 100)

    def test_set_formatter(self):
        with namedTemporaryDir() as tmpdir:
            filename = os.path.join(tmpdir, "test.log")
            with threaded_handler(filename, 10) as (handler, logger):
                formatter = logging.Formatter("--%(message)s--")
                handler.setFormatter(formatter)
                logger.info("message")
            with open(filename) as f:
                self.assertEqual(f.read(), "--message--\n")
