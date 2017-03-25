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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301 USA
#
# Refer to the README and COPYING files for full details of the license
#

import io
import logging
import os
import shutil
import tempfile
import time

from vdsm import cmdutils
from vdsm import commands
from vdsm import udevadm
from vdsm.common import concurrent

from vdsm.virt import dmstats

import loopback
from testValidation import skipif
from testValidation import slowtest
from testlib import VdsmTestCase

MB = 1024**2
GB = 1024**3

temp_dir = None
loop_device = None

VG_NAME = "dmstats-test-vg"
LV_NAME = "dmstats-test-lv"
LV_SIZE = 2 * GB


def setup_module():
    # create a backing file and mount it on a free loop device, then create a
    # temporary vg and lv
    global temp_dir, loop_device
    if os.geteuid() != 0:
        return
    temp_dir = tempfile.mkdtemp(prefix="dmstats_test.", dir="/dev/shm")
    try:
        backing_file = os.path.join(temp_dir, "backing_file")
        with open(backing_file, "w") as f:
            f.truncate(10 * 1024**3)
        loop_device = loopback.Device(backing_file)
        loop_device.attach()
        try:
            lvm("pvcreate", loop_device.path)
            try:
                udevadm.settle(5)
                lvm("vgcreate", VG_NAME, loop_device.path)
                try:
                    udevadm.settle(5)
                    lvm("lvcreate", "--name", LV_NAME, "--size", "%db"
                        % LV_SIZE, VG_NAME)
                except:
                    lvm("vgremove", VG_NAME)
                    raise
            except:
                lvm("pvremove", loop_device.path)
                raise
        except:
            loop_device.detach()
            raise
    except:
        shutil.rmtree(temp_dir)
        raise


def teardown_module():
    # remove temporary vg and lv and the underling loop device and backing file
    if os.geteuid() != 0:
        return
    lvm("lvchange", "--available", "n", "%s/%s" % (VG_NAME, LV_NAME))
    lvm("lvremove", "%s/%s" % (VG_NAME, LV_NAME))
    lvm("vgremove", VG_NAME)
    lvm("pvremove", loop_device.path)
    loop_device.detach()
    shutil.rmtree(temp_dir)


class TestDmstats(VdsmTestCase):

    @skipif(os.geteuid() != 0, reason="needs root")
    def test_add_and_remove(self):
        threshold = GB
        monitor = dmstats.Monitor()
        monitor.add(VG_NAME, LV_NAME, threshold)
        try:
            watches = monitor.list()
            self.assertEqual(watches, [
                dmstats.Region(VG_NAME, LV_NAME, LV_SIZE - threshold,
                               threshold)
            ])
        finally:
            monitor.remove(VG_NAME, LV_NAME)

    @skipif(os.geteuid() != 0, reason="needs root")
    def test_update_region(self):
        monitor = dmstats.Monitor()
        monitor.add(VG_NAME, LV_NAME, GB)
        try:
            threshold = GB // 2
            monitor.add(VG_NAME, LV_NAME, threshold)
            watches = monitor.list()
            self.assertEqual(watches, [
                dmstats.Region(VG_NAME, LV_NAME, LV_SIZE - threshold,
                               threshold)
            ])
        finally:
            monitor.remove(VG_NAME, LV_NAME)

    @skipif(os.geteuid() != 0, reason="needs root")
    @slowtest
    def test_monitor_events(self):
        # TODO: Break to simple short test testing one dsmstat operation.
        # Testing monitor flows should be done using fake events from dmstat.
        monitor = dmstats.Monitor()
        events = []

        def watcher():
            for event in monitor:
                events.append(event)

        monitor.start()
        t = concurrent.thread(watcher, name="watcher")
        t.start()
        try:
            monitor.add(VG_NAME, LV_NAME, GB)
            try:
                # Write data before watermark region - no event should be
                # received.
                write_to_lv(LV_SIZE - GB - MB, MB)
                time.sleep(2)
                self.assertEqual(events, [])

                # Write data to watermark region - should receive one event.
                write_to_lv(LV_SIZE - GB, MB)
                time.sleep(2)
                self.assertEqual(events, [
                    dmstats.Event(VG_NAME, LV_NAME, LV_SIZE - GB, GB, 2048)
                ])
                del events[:]

                # Update region and write data before watermark region - should
                # not receive any event.
                monitor.add(VG_NAME, LV_NAME, GB // 2)
                write_to_lv(LV_SIZE - GB // 2 - MB, MB)
                time.sleep(2)
                self.assertEqual(events, [])

                # Write data to watermark region - should receive one event.
                write_to_lv(LV_SIZE - GB // 2, MB)
                time.sleep(2)
                self.assertEqual(events, [
                    dmstats.Event(VG_NAME, LV_NAME, LV_SIZE - GB // 2, GB // 2,
                                  2048)
                ])
                del events[:]

            finally:
                monitor.remove(VG_NAME, LV_NAME)
        finally:
            monitor.stop()
            monitor.wait()
            t.join()


def write_to_lv(offset, size):
    log = logging.getLogger("test")
    log.debug("Writing %d bytes at %d", size, offset)
    lv_path = os.path.join("/dev", VG_NAME, LV_NAME)
    with io.open(lv_path, "wb", buffering=0) as f:
        f.seek(offset)
        f.write(b"x" * size)
        os.fsync(f.fileno())


def lvm(*args):
    command = list(args)
    # This is the magic to make lvcreate succeed inside a container, see
    # https://groups.google.com/forum/#!topic/docker-user/n4Xtvsb4RAw
    command[1:1] = ["--config", "activation { udev_sync=0 udev_rules=0 }"]
    return run(*command)


def run(*cmd):
    rc, out, err = commands.execCmd(cmd, raw=True)
    if rc != 0:
        raise cmdutils.Error(cmd, rc, out, err)
    return out
