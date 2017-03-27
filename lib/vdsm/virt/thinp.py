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
thinp - thin provioning block storge
====================================

This module will manages block storage thin provioned disks.

Currnetly, we only start the dsmstats monitor, and log events.
"""

from __future__ import absolute_import

import logging
from vdsm.common import concurrent

log = logging.getLogger("virt.thinp")


class Extender(object):

    def __init__(self, monitor):
        self._monitor = monitor
        self._thread = concurrent.thread(self._run, name="thinp")

    def start(self):
        log.info("Starting extender")
        self._thread.start()

    def stop(self):
        log.info("Stopping extender")
        self._monitor.stop()

    def wait(self, timeout=None):
        self._thread.join(timeout)
        return self._thread.is_alive()

    def add_volume(self, sd_id, vol_id, threshold):
        log.info("Start monitoring volume watermark (sd_id=%s, vol_id=%s, "
                 "threshold=%d)",
                 sd_id, vol_id, threshold)
        self._monitor.add(sd_id, vol_id, threshold)

    def remove_volume(self, sd_id, vol_id):
        log.info("Stop monitoring volume watermark (sd_id=%s, vol_id=%s)",
                 sd_id, vol_id)
        self._monitor.remove(sd_id, vol_id)

    def _run(self):
        log.info("Extender started")
        self._monitor.start()
        for event in self._monitor:
            try:
                self._extend(event)
            except Exception as e:
                log.exception("Error extending volume: %s", e)
        log.info("Extender terminated")

    def _extend(self, event):
        log.info("Extending event %s", event)


_extender = None


def start(monitor):
    """
    Start the thinp extender.
    """
    global _extender
    assert _extender is None
    _extender = Extender(monitor)
    _extender.start()


def stop():
    """
    Stop the thinp extender.
    """
    if _extender:
        _extender.stop()


def wait(timeout=None):
    """
    Wait until the thinp extender is stopped or timeout expires.

    Return True if the extender was stopped, False if it is still running when
    timeout expire.
    """
    global _extender
    if _extender is None:
        return True
    if _extender.wait(timeout):
        _extender = None
        return True
    return False


def add_volume(sd_id, vol_id, threshold):
    _extender.add_volume(sd_id, vol_id, threshold)


def remove_volume(sd_id, vol_id):
    _extender.remove_volume(sd_id, vol_id)
