#
# Copyright 2016 Red Hat, Inc.
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
statedir - manage /var/lib/storage/xxx state files.

This module persist storage configuration for debugging or connecting to
storage when engine is down.
"""

from __future__ import absolute_import

import errno
import io
import logging
import os

from vdsm import constants

BASE_DIR = os.path.join(constants.P_VDSM_LIB, "storage")

log = logging.getLogger("storage.statedir")


def create():
    """
    Create the statedir directory if needed
    """
    try:
        os.makedirs(BASE_DIR)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
        log.debug("Using statedir %r", BASE_DIR)
    else:
        log.info("Created statedir %r", BASE_DIR)


def write(key, value):
    """
    Store value in /var/lib/storage/key
    """
    path = os.path.join(BASE_DIR, key)
    log.info("Storing %r=%r", key, value)
    with io.open(path, "wb") as f:
        f.write(b"%s\n" % value)
