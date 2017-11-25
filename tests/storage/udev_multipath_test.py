#
# Copyright 2009-2017 Red Hat, Inc.
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

import glob
import os
import pprint
import threading

import pytest

from vdsm.storage import udev
from vdsm.utils import running

import loopback

EVENT = udev.MultipathEvent(type=udev.MPATH_REMOVED,
                            mpath_uuid="fake-uuid-3",
                            path=None,
                            valid_paths=None)


class FakeDevice(dict):
    @property
    def action(self):
        return self["action"]

DEVICE = FakeDevice(DM_UUID="mpath-fake-uuid-3",
                    action="remove")


class Monitor(object):
    """
    A testing monitor, keeping received events.

    This monitor is very strict - can be started and stopped only once, for
    verifing correct monitor lifecycle managment. Real monitor may support
    starting and stopping multiple times.
    """

    CREATED = "created"
    STARTED = "started"
    STOPPED = "stopped"

    def __init__(self):
        self.calls = []
        self.state = self.CREATED

    def start(self):
        assert self.state == self.CREATED
        self.state = self.STARTED

    def handle(self, event):
        self.calls.append(event)

    def stop(self):
        assert self.state == self.STARTED
        self.state = self.STOPPED


class MonitorError(Exception):
    """ Raised by bad monitors. """


class UnstartableMonitor(Monitor):

    def start(self):
        raise MonitorError("No start for you!")


class BadMonitor(Monitor):

    def handle(self, event):
        raise MonitorError("No event for you!")


class UnstopableMonitor(Monitor):

    def stop(self):
        raise MonitorError("No stop for you!")


def fake_block_device_name(dev):
    return "sda"


def test_start():
    listener = udev.MultipathListener()
    try:
        listener.start()
    except Exception as e:
        pytest.fail("Unexpected Exception: %s", e)
    else:
        listener.stop()


def test_start_twice():
    listener = udev.MultipathListener()
    listener.start()
    with pytest.raises(AssertionError):
        listener.start()
    listener.stop()


def test_stop():
    listener = udev.MultipathListener()
    listener.start()
    try:
        listener.stop()
    except Exception as e:
        pytest.fail("Unexpected Exception: %s", e)


def test_stop_twice():
    listener = udev.MultipathListener()
    listener.start()
    listener.stop()
    try:
        listener.stop()
    except Exception as e:
        pytest.fail("Unexpected Exception: %s", e)


def test_monitor_lifecycle():
    listener = udev.MultipathListener()
    monitors = [Monitor(), Monitor()]
    for m in monitors:
        listener.register(m)

    # Starting the listener starts the monitors.
    with running(listener):
        for m in monitors:
            assert m.state == Monitor.STARTED

    # Stopping the listener stops the monitors.
    for m in monitors:
        assert m.state == Monitor.STOPPED


def test_monitor_lifecycle_start_error():

    def check(*monitors):
        listener = udev.MultipathListener()
        for m in monitors:
            listener.register(m)
        with pytest.raises(MonitorError):
            listener.start()

    bad_mon = UnstartableMonitor()
    good_mon = Monitor()
    check(good_mon, bad_mon)
    assert bad_mon.state == Monitor.CREATED
    assert good_mon.state in (Monitor.CREATED, Monitor.STOPPED)

    bad_mon = UnstartableMonitor()
    good_mon = Monitor()
    check(bad_mon, good_mon)
    assert bad_mon.state == Monitor.CREATED
    assert good_mon.state in (Monitor.CREATED, Monitor.STOPPED)


def test_monitor_lifecycle_stop_error():

    def check(*monitors):
        listener = udev.MultipathListener()
        for m in monitors:
            listener.register(m)
        with running(listener):
            pass

    bad_mon = UnstopableMonitor()
    good_mon = Monitor()
    check(good_mon, bad_mon)
    assert bad_mon.state == Monitor.STARTED
    assert good_mon.state == Monitor.STOPPED

    bad_mon = UnstopableMonitor()
    good_mon = Monitor()
    check(bad_mon, good_mon)
    assert bad_mon.state == Monitor.STARTED
    assert good_mon.state == Monitor.STOPPED


def test_hotplug_monitor():
    # Registering a monitor after the listenr was started will start the
    # monitor after registering it. The monitor must be able to handle events
    # while the monitor is starting.
    listener = udev.MultipathListener()
    with running(listener):
        mon = Monitor()
        listener.register(mon)
        assert mon.state == Monitor.STARTED

        listener._callback(DEVICE)
        assert mon.calls == [EVENT]


def test_hotplug_monitor_error():

    listener = udev.MultipathListener()
    with running(listener):
        mon = UnstartableMonitor()
        # Monitor start() error should raised
        with pytest.raises(MonitorError):
            listener.register(mon)

        assert mon.state == Monitor.CREATED

        # Monitor should not be registered.
        listener._callback(DEVICE)
        assert mon.calls == []


def test_hotunplug_monitor():
    # When unregistring a monitor while the listener is running, we should stop
    # it, since the listener started it.
    listener = udev.MultipathListener()
    with running(listener):
        mon = Monitor()
        listener.register(mon)
        listener.unregister(mon)
        assert mon.state == Monitor.STOPPED


@pytest.mark.parametrize("device,expected", [
    (
        # Multipath path is restored
        FakeDevice(
            action="change",
            DM_ACTION="PATH_REINSTATED",
            DM_UUID="mpath-fake-uuid-1",
            DM_PATH="sda",
            DM_NR_VALID_PATHS="1"),
        udev.MultipathEvent(
            type=udev.PATH_REINSTATED,
            mpath_uuid="fake-uuid-1",
            path="sda",
            valid_paths=1)
    ),
    (
        # Multipath path has failed
        FakeDevice(
            action="change",
            DM_ACTION="PATH_FAILED",
            DM_UUID="mpath-fake-uuid-2",
            DM_PATH="66:32",
            DM_NR_VALID_PATHS="4"),
        udev.MultipathEvent(
            type=udev.PATH_FAILED,
            mpath_uuid="fake-uuid-2",
            path="sda",
            valid_paths=4)
    ),
    (
        # Multipath device has been removed
        FakeDevice(
            action="remove",
            DM_UUID="mpath-fake-uuid-3"),
        udev.MultipathEvent(
            type=udev.MPATH_REMOVED,
            mpath_uuid="fake-uuid-3",
            path=None,
            valid_paths=None)
    ),
])
def test_report_events(device, expected):
    listener = udev.MultipathListener()
    # Avoid accessing non-existing devices
    listener._block_device_name = fake_block_device_name
    mon = Monitor()
    listener.register(mon)
    listener._callback(device)

    assert mon.calls == [expected]


@pytest.mark.parametrize("device", [
    # the DM_UUID does not start with "mpath"
    FakeDevice(action="change",
               DM_UUID="usb-fake-uuid-1"),
    # the DM_ACTION is not supported
    FakeDevice(action="change",
               DM_UUID="mpath-fake-uuid-2",
               DM_ACTION="PATH_DISINTEGRATED",
               DM_NR_VALID_PATHS="4"),
    # the "action" is not supported
    FakeDevice(action="update",
               DM_UUID="mpath-fake-uuid-3")
])
def test_filter_event(device):
    listener = udev.MultipathListener()
    mon = Monitor()
    listener.register(mon)
    listener._callback(device)

    assert mon.calls == []


def test_monitor_unregistered():
    listener = udev.MultipathListener()
    mon = Monitor()
    listener.register(mon)
    listener._callback(DEVICE)
    assert mon.calls == [EVENT]
    listener.unregister(mon)
    listener._callback(DEVICE)
    assert mon.calls == [EVENT]


def test_monitor_not_registered():
    listener = udev.MultipathListener()
    with pytest.raises(AssertionError):
        listener.unregister(None)


def test_monitor_already_registered():
    listener = udev.MultipathListener()
    listener.register(None)
    with pytest.raises(AssertionError):
        listener.register(None)


def test_monitor_exception():

    def check(*monitors):
        listener = udev.MultipathListener()
        for m in monitors:
            listener.register(m)
        listener._callback(DEVICE)

    bad_mon = BadMonitor()
    good_mon = Monitor()
    check(good_mon, bad_mon)
    assert good_mon.calls == [EVENT]

    bad_mon = BadMonitor()
    good_mon = Monitor()
    check(bad_mon, good_mon)
    assert good_mon.calls == [EVENT]


def test_register_from_callback():
    listener = udev.MultipathListener()
    mon2 = Monitor()

    class Adder(Monitor):
        def handle(self, event):
            listener.register(mon2)

    mon1 = Adder()
    listener.register(mon1)
    listener._callback(DEVICE)
    listener._callback(DEVICE)
    assert mon2.calls == [EVENT]


def test_unregister_from_callback():
    listener = udev.MultipathListener()

    class Remover(Monitor):
        def handle(self, event):
            self.calls.append(event)
            listener.unregister(self)

    mon = Remover()
    listener.register(mon)
    listener._callback(DEVICE)
    listener._callback(DEVICE)

    assert mon.calls == [EVENT]


def test_failing_event():
    fd = FakeDevice(
        action="change",
        DM_ACTION="PATH_REINSTATED",
        DM_UUID="mpath-fake-uuid-1",
        DM_PATH="sda",
        DM_NR_VALID_PATHS="sfsdfs")
    listener = udev.MultipathListener()
    mon = Monitor()
    listener.register(mon)
    listener._callback(fd)

    listener._callback(DEVICE)
    assert mon.calls == [EVENT]


def test_block_device_name():
    devs = glob.glob("/sys/block/*/dev")
    dev_name = os.path.basename(os.path.dirname(devs[0]))
    with open(devs[0], 'r') as f:
        major_minor = f.readline().rstrip()
        listener = udev.MultipathListener()
        assert listener._block_device_name(major_minor) == dev_name


@pytest.mark.xfail(
    os.environ.get("OVIRT_CI") or os.environ.get("TRAVIS_CI"),
    reason="Requires real env")
@pytest.mark.skipif(os.geteuid() != 0, reason="Requires root")
def test_loopback_event(tmpdir):
    listener = udev.MultipathListener()
    received = threading.Event()
    devices = []

    def callback(device):
        pprint.pprint({k: device[k] for k in device})
        devices.append(device)
        received.set()

    listener._callback = callback
    with running(listener):
        # Create a backing file
        filename = str(tmpdir.join("file"))
        with open(filename, "wb") as f:
            f.truncate(1024**2 * 10)

        # Create and remove a loop device
        with loopback.Device(filename) as loop:
            print("Created a loop device at %r" % loop.path)
            if not received.wait(1):
                raise RuntimeError("Timeout receiving event")

            # We expect an event about our loop device
            assert devices[0].get("DEVNAME") == loop.path
