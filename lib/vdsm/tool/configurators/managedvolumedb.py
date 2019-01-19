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

import logging
import os
import sqlite3
from contextlib import closing

from vdsm import constants
from vdsm.common import errors
from vdsm.storage import managedvolumedb as db

from . import YES, MAYBE


log = logging.getLogger("tool.configurators.managevolumedb")


class DatabaseError(errors.Base):

    msg = "Managed volumes database error, see log for details"


def configure():
    """
    Create database for managed volumes
    """
    res = _check_db_version()
    if res == 0:
        return
    elif res == 1:
        db.create_db()
        os.chown(db.DB_FILE, constants.VDSM_USER, constants.VDSM_GROUP)
    else:
        raise DatabaseError


def isconfigured():
    """
    Return YES if managedvolumedb is configured, otherwise MAYBE as we don't
    want to crash vdsm install if creation of db fails.
    """
    if _check_db_version() == 0:
        return YES
    else:
        return MAYBE


def _check_db_version():

    # DB file doesn't exists
    if not os.path.isfile(db.DB_FILE):
        log.info("DB file %s doesn't exists", db.DB_FILE)
        return 1

    # check DB file ownership
    if os.stat(db.create_db()).st_uid != constants.VDSM_USER or os.stat(
            db.create_db()).st_gid != constants.VDSM_GROUP:
        log.warn("DB file %s hasn't proper ownership %s:%s", db.DB_FILE,
                 constants.VDSM_USER, constants.VDSM_GROUP)
        return 2

    # check it has correct tables
    conn = sqlite3.connect(db.DB_FILE)
    with closing(conn):
        conn.row_factory = sqlite3.Row
        res = conn.execute("SELECT name FROM sqlite_master")
        tables = res.fetchone()
        if "volumes" not in tables:
            log.info("Table 'volumes' not found in DB tables")
            return 1
        if "versions" not in tables:
            log.info("Table 'versions' not found in DB tables")
            return 1

    try:
        # check version is expected one
        version = db.version_info()
        log.debug("Database version=%s", version["version"])
        if db.VERSION == version["version"]:
            return 0
        else:
            log.warn("Database version (%s) is not the same as expected "
                     "one (%s)", version["version"], db.VERSION)
            return 2
    except Exception as e:
        log.warn("Failed to query database version: %s", str(e))
        return 2
