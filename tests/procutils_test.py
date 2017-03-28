#
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

from __future__ import print_function

import io
import os
import subprocess

from vdsm import commands
from vdsm import utils
from vdsm.compat import CPopen
from vdsm.common import procutils

from testValidation import slowtest
from testlib import VdsmTestCase


class TestCommunicate(VdsmTestCase):

    def test_no_output_success(self):
        p = CPopen(["true"],
                   stdin=None,
                   stdout=subprocess.PIPE,
                   stderr=subprocess.PIPE)
        received = list(procutils.communicate(p))
        self.assertEqual(received, [])
        self.assertEqual(p.returncode, 0)

    def test_no_output_error(self):
        p = CPopen(["false"],
                   stdin=None,
                   stdout=subprocess.PIPE,
                   stderr=subprocess.PIPE)
        received = list(procutils.communicate(p))
        self.assertEqual(received, [])
        self.assertEqual(p.returncode, 1)

    def test_stdout(self):
        p = CPopen(["echo", "output"],
                   stdin=None,
                   stdout=subprocess.PIPE,
                   stderr=subprocess.PIPE)
        received = list(procutils.communicate(p))
        self.assertEqual(received, [(procutils.OUT, b"output\n")])
        self.assertEqual(p.returncode, 0)

    def test_stderr(self):
        p = CPopen(["sh", "-c", "echo error >/dev/stderr"],
                   stdin=None,
                   stdout=subprocess.PIPE,
                   stderr=subprocess.PIPE)
        received = list(procutils.communicate(p))
        self.assertEqual(received, [(procutils.ERR, b"error\n")])
        self.assertEqual(p.returncode, 0)

    def test_both_stdout_stderr(self):
        p = CPopen(["sh", "-c", "echo output; echo error >/dev/stderr;"],
                   stdin=None,
                   stdout=subprocess.PIPE,
                   stderr=subprocess.PIPE)
        received = list(procutils.communicate(p))
        self.assertEqual(sorted(received), sorted([
            (procutils.OUT, b"output\n"), (procutils.ERR, b"error\n")
        ]))
        self.assertEqual(p.returncode, 0)

    def test_timeout(self):
        p = CPopen(["sleep", "1"],
                   stdin=None,
                   stdout=subprocess.PIPE,
                   stderr=subprocess.PIPE)
        try:
            with self.assertRaises(procutils.TimeoutExpired):
                for _ in procutils.communicate(p, 0.5):
                    pass
            self.log.debug("Process (pid=%d) terminated", p.pid)
        finally:
            p.kill()
            p.wait()

    def test_no_fds(self):
        p = CPopen(["sleep", "1"],
                   stdin=None,
                   stdout=None,
                   stderr=None)
        try:
            with self.assertRaises(procutils.TimeoutExpired):
                for _ in procutils.communicate(p, 0.5):
                    pass
            self.log.debug("Process (pid=%d) terminated", p.pid)
        finally:
            p.kill()
            p.wait()

    def test_fds_closed(self):
        cmd = ["python", "-c",
               "import os, time; os.close(1); os.close(2); time.sleep(1)"]
        p = CPopen(cmd, stdin=None, stdout=subprocess.PIPE,
                   stderr=subprocess.PIPE)
        try:
            with self.assertRaises(procutils.TimeoutExpired):
                for _ in procutils.communicate(p, 0.5):
                    pass
            self.log.debug("Process (pid=%d) terminated", p.pid)
        finally:
            p.kill()
            p.wait()


class TestBench(VdsmTestCase):

    COUNT = 1024
    BUFSIZE = 1024**2

    def test_plain_read(self):
        p = CPopen(["dd", "if=/dev/zero", "bs=%d" % self.BUFSIZE,
                    "count=%d" % self.COUNT],
                   stdin=None,
                   stdout=subprocess.PIPE,
                   stderr=subprocess.PIPE)
        start = utils.monotonic_time()
        received = 0
        while True:
            data = os.read(p.stdout.fileno(), self.BUFSIZE)
            if not data:
                break
            received += len(data)
        p.wait()
        elapsed = utils.monotonic_time() - start
        received_gb = received / float(1024**3)
        print("%.2fg in %.2f seconds (%.2fg/s)"
              % (received_gb, elapsed, received_gb / elapsed), end=" ")
        self.assertEqual(received, self.COUNT * self.BUFSIZE)
        self.assertEqual(p.returncode, 0)

    def test_read(self):
        p = CPopen(["dd", "if=/dev/zero", "bs=%d" % self.BUFSIZE,
                    "count=%d" % self.COUNT],
                   stdin=None,
                   stdout=subprocess.PIPE,
                   stderr=subprocess.PIPE)
        start = utils.monotonic_time()
        received = 0
        for src, data in procutils.communicate(p, bufsize=self.BUFSIZE):
            if src == procutils.OUT:
                received += len(data)
        elapsed = utils.monotonic_time() - start
        received_gb = received / float(1024**3)
        print("%.2fg in %.2f seconds (%.2fg/s)"
              % (received_gb, elapsed, received_gb / elapsed), end=" ")
        self.assertEqual(received, self.COUNT * self.BUFSIZE)
        self.assertEqual(p.returncode, 0)

    def test_write(self):
        p = CPopen(["dd", "of=/dev/null", "bs=%d" % self.BUFSIZE],
                   stdin=subprocess.PIPE,
                   stdout=None,
                   stderr=subprocess.PIPE)
        start = utils.monotonic_time()
        total = self.COUNT * self.BUFSIZE
        sent = 0
        with io.open("/dev/zero", "rb") as f:
            while sent < total:
                n = min(total - sent, self.BUFSIZE)
                data = f.read(n)
                if not data:
                    raise RuntimeError("/dev/zero closed?!")
                p.stdin.write(data)
                sent += len(data)
        p.stdin.flush()
        p.stdin.close()
        for _, data in procutils.communicate(p, 10):
            pass
        elapsed = utils.monotonic_time() - start
        sent_gb = sent / float(1024**3)
        print("%.2fg in %.2f seconds (%.2fg/s)"
              % (sent_gb, elapsed, sent_gb / elapsed), end=" ")
        self.assertEqual(p.returncode, 0)

    @slowtest
    def test_asyncproc_read(self):
        p = commands.execCmd(["dd", "if=/dev/zero", "bs=%d" % self.BUFSIZE,
                              "count=%d" % self.COUNT],
                             sync=False, raw=True)
        start = utils.monotonic_time()
        p.blocking = True
        received = 0
        while True:
            data = p.stdout.read(self.BUFSIZE)
            if not data:
                break
            received += len(data)
        p.wait()
        elapsed = utils.monotonic_time() - start
        received_gb = received / float(1024**3)
        print("%.2fg in %.2f seconds (%.2fg/s)"
              % (received_gb, elapsed, received_gb / elapsed), end=" ")
        self.assertEqual(received, self.COUNT * self.BUFSIZE)
        self.assertEqual(p.returncode, 0)

    @slowtest
    def test_asyncproc_write(self):
        p = commands.execCmd(["dd", "of=/dev/null", "bs=%d" % self.COUNT],
                             sync=False, raw=True)
        start = utils.monotonic_time()
        total = self.COUNT * self.BUFSIZE
        sent = 0
        with io.open("/dev/zero", "rb") as f:
            while sent < total:
                n = min(total - sent, self.BUFSIZE)
                data = f.read(n)
                if not data:
                    raise RuntimeError("/dev/zero closed?!")
                p.stdin.write(data)
                sent += len(data)
        p.stdin.flush()
        p.stdin.close()
        p.wait()
        elapsed = utils.monotonic_time() - start
        sent_gb = sent / float(1024**3)
        print("%.2fg in %.2f seconds (%.2fg/s)"
              % (sent_gb, elapsed, sent_gb / elapsed), end=" ")
        self.assertEqual(p.returncode, 0)
