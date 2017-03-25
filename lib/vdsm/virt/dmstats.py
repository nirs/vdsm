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

"""
dmstats - monitor thin block volumes with dmstats
=================================================

This module implement watermark monitoring using the dmstats program.

Theory of operation
-------------------

Before a thin provisioned block volume is used, create a watermark
region at the end of the device. Assuming a device of 10g and watermark
threshold of 1g::

    dmstats create \
        --programid ovirt-watermark \
        --start 9g \
        --length 1g \
        vg-name/lv-name

Watermarks regions are using the ovirt-watermark program id, so we can
remove the watermark regions easily from single device or from list of
devices.

The region is removed when device is tore down, or after a device was
extended::

    dmstats delete \
        --programid ovirt-watermark \
        --allregions \
        vg-name/lv-name

If vms are using thin provisioned block volumes, dmstats.Monitor is
started, watching volume watermark every second::

    dmstats report \
        --programid ovirt-watermark \
        --interval 1 \
        --noheadings \
        --nosuffix \
        --units b \
        --select "write_sector_count != 0" \
        -o vg_name,lv_name,region_start,region_len,write_sector_count

If a vm writes into the watermark region of a lv dmstats will report
this lv.  The report is terminated by a blank line::

    vg-name:lv-name:9663676416:1073741824:204800\n
    \n

If no region was modified during the current internal, dmstats report an
empty line. The monitor does not report any events in this case.

Virt drive monitoring thread should wait for events and extend a volume
when an event is received.

dmstats does not keep device statistics after reporting them, so we may
get a single event for a watermark region. Once we got the event, if the
vm stopped writing to the volume, dmstats will not report this device
again.  Virt code monitoring devices must fetch the current device
allocation from libvirt when starting to monitor devices.

The dmstats commands must run as root. We are using sudo so we can run
both the commands and the montior inside vdsm.


Watching for watermark events
-----------------------------

To watch for watermark events, you need to create and start a
dmstats.Monitor::

    monitor = dmstats.Monitor()
    monitor.start()

Then you should start a thread, reading events from the monitor, and
sending extend requests for the volume::

    for event in monitor:
        # Extend the volume...

Finally, stop the monitor::

    monitor.stop()


Watermark events
----------------

A wartermark event contains these attributes:

- length - length of the watermark region in bytes
- lv_name - device lv name
- offset - offset of the watermark region from start of volume in bytes.
- vg_name - device vg name
- write_sector_count - secotrs written in the last monitor interval

When an event is received, the actual volume allocation is somewhere
between the region.offset and end of the device.

You will typically get multiple events for the same volume, depending on
how fast the vm writes data to the volume, and how slow the extend
process is. The volume extending code should handle duplicate extend
requests.


Start monitoring a volume
-------------------------

When preparing a volume, before starting a live merge into the base
volume, or before starting replication during live storage migration,
you need to start monitoring the volume watermark.

    dmstats.add(vg_name, lv_name, threshold=1024**3)

This will create a new region at the end of the device. A watermark
events may be received immediately before this call returns.


Stopping monitoring a volume
----------------------------

When tearing down a volume, after live merge has completed, or after
live storage migration has completed, you need to stop monitoring the
volume watermark.

    dmstats.remove(vg_name, lv_name)

This will delete the watermark region for this device. No more events
for this device will be received.


Updating after a volume was extended
------------------------------------

After a volume was extended or resized, add the device again.
dmstatus.add() will remove the old region and create a new region at the
end of the device.


Listing monitored devices
-------------------------

To report which devices are monitored, use:

    dmstats.list()

This returns a list of `dmstats.Region` instances, once per each
monitored device.
"""

from __future__ import absolute_import

import collections
import io
import logging
import os
import subprocess

from vdsm import cmdutils
from vdsm import commands
from vdsm import constants
from vdsm.common import cmdutils as common_cmdutils

from vdsm.common.compat import CPopen

PROGRAM_ID = "ovirt-watermark"
SEPARATOR = ":"

log = logging.getLogger("virt.dmstats")


Region = collections.namedtuple("Region", [
    "vg_name", "lv_name", "start", "length"
])


Event = collections.namedtuple("Event", [
    "vg_name", "lv_name", "start", "length", "write_secotr_count"
])


class Monitor(object):

    def __init__(self):
        self._proc = None
        self._stopped = False

    # Running

    def start(self):
        assert self._proc is None
        log.info("Starting dmstats monitor")

        # dmstats does not flush stdout after each report, use stdbuf to make
        # dmstats's stdio line buffered.
        # TODO: Fixed in upstream (b8b2b1efd84fb1e96500d86f2ada3c2efa1338f3),
        # remove when the fix is available in all platforms.
        cmd = [constants.EXT_STDBUF, "--output", "L",
               constants.EXT_DMSTATS,
               "report",
               "--programid", PROGRAM_ID,
               "--interval", "1",
               "--noheadings",
               "--nosuffix",
               "--units", "b",
               "--select", "write_sector_count != 0",
               "-o", ("vg_name,lv_name,region_start,region_len,"
                      "write_sector_count")]

        cmd = cmdutils.wrap_command(cmd, with_sudo=True)
        log.debug(common_cmdutils.command_log_line(cmd))
        self._proc = CPopen(cmd,
                            stdin=None,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)
        log.debug("Started dmstats (pid=%d) monitor", self._proc.pid)

    def stop(self):
        if self._proc is None:
            return
        log.info("Stopping dmstats (pid=%d) monitor", self._proc.pid)
        self._stopped = True
        if self._proc.poll() is None:
            # dmstats as root, so Popen.terminate() is not effective
            cmd = [constants.EXT_KILL, str(self._proc.pid)]
            commands.execCmd(cmd, sudo=True)

    def wait(self):
        if self._proc is None:
            return
        self._proc.wait()
        self._proc = None

    # Events

    def __iter__(self):
        log.debug("Waiting for dmstats events...")
        for line in iter(self._proc.stdout.readline, b""):
            if line == b"\n":
                continue
            # vg_name:lv_name:start:length:write_secotr_count
            line = line.decode("ascii")
            vg_name, lv_name, start, length, wsc = line.split(SEPARATOR)
            event = Event(vg_name, lv_name, int(start), int(length), int(wsc))
            log.debug("Received event %s", event)
            yield event

        _, err = self._proc.communicate()
        if not self._stopped and self._proc.returncode != 0:
            log.error("dmstats failed (rc=%d, err=%r)",
                      self._proc.returncode, err)

    # Regions

    def add(self, vg_name, lv_name, threshold):
        """
        Add watermark region for a device.

        Argumnets:
            vg_name (str): vg name
            lv_name (str): lv name
            threshold (int): the size of the watermark at the end of the
                device in bytes. If anything is written into this area,
                an event will be receied on the `dmstats.Monitor`.
        Raises:
            `cmdutils.Error` if underlying dmstats program failed
        """
        size = _device_size(vg_name, lv_name)
        start = max(0, size - threshold)
        length = size - start
        log.info("Adding watermark region (vg_name=%s, lv_name=%s, start=%d, "
                 "length=%s)",
                 vg_name, lv_name, start, length)
        # dmstats will happily add multiple identical regions, so we first
        # remove possibly stale regions.
        _remove_watermark_regions(vg_name, lv_name)
        _dmstats(
            "create",
            "--programid", PROGRAM_ID,
            "--start", "%db" % start,
            "--length", "%db" % length,
            "%s/%s" % (vg_name, lv_name)
        )

    def remove(self, vg_name, lv_name):
        """
        Remove watermark region for a device.

        Argumnets:
            vg_name (str): vg name
            lv_name (str): lv name

        Raises:
            `cmdutils.Error` if underlying dmstats program failed
        """
        log.info("Removing watermark region (vg_name=%s, lv_name=%s)",
                 vg_name, lv_name)
        _remove_watermark_regions(vg_name, lv_name)

    def list(self):
        """
        List watermark regions.

        Returns:
            list of `Region` objects

        Raises:
            `cmdutils.Error` if underlying dmstats program failed
        """
        out = _dmstats(
            "list",
            "--programid", PROGRAM_ID,
            "--units", "b",
            "--nosuffix",
            "--noheadings",
            "-o", "vg_name,lv_name,region_start,region_len",
        )
        out = out.decode("ascii").strip()
        regions = []
        for line in out.splitlines():
            vg_name, lv_name, start, length = line.split(SEPARATOR)
            r = Region(vg_name, lv_name, int(start), int(length))
            regions.append(r)
        return regions


def _remove_watermark_regions(vg_name, lv_name):
    # This should use --programid ovirt-watermark, but this is broken on
    # rhel 7.3, missing 5eda3934885b23ce06f862a56b524ceaab3cb565.  Until
    # this fix is available, remove all programs regions. Since we don't
    # have other regions yet it is ok.
    _dmstats(
        "delete",
        "--allprograms",
        "--allregions",
        "%s/%s" % (vg_name, lv_name)
    )


def _device_size(vg_name, lv_name):
    """
    Return the size of an active lv.
    """
    path = os.path.join("/dev", vg_name, lv_name)
    # TODO: Should use fsutils.size(), but it is in the storage package.
    with io.open(path, "rb") as f:
        f.seek(0, os.SEEK_END)
        return f.tell()


def _dmstats(*args):
    cmd = [constants.EXT_DMSTATS]
    cmd.extend(args)
    rc, out, err = commands.execCmd(cmd, sudo=True, raw=True)
    if rc != 0:
        raise cmdutils.Error(cmd, rc, out, err)
    return out
