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

"""
managevolumedb - stores connection details about managed volumes
"""

from __future__ import absolute_import
from __future__ import division

import datetime
import json
import logging
import os
import threading

from contextlib import closing

import lmdb

from vdsm.common import errors
from vdsm.storage import constants as sc

VERSION = 1
DB_FILE = os.path.join(sc.P_VDSM_LIB, "managedvolume.db")

# Maximum database file size.
MAP_SIZE = 10 * 1024**2

# Number of databases in the global env.
MAX_DBS = 3

# Databases names.
VOLUMES_DB = b"volumes"
MULTIPATHS_DB = b"multipaths"
VERSIONS_DB = b"versions"

# Public interface.


class NotFound(errors.Base):

    msg = "Managed volume with vol_id {self.vol_id} not found"

    def __init__(self, vol_id):
        self.vol_id = vol_id


class VolumeAlreadyExists(errors.Base):

    msg = ("Failed to store {self.vol_info}."
           "Volume with id {self.vol_id} already exists in the DB")

    def __init__(self, vol_id, vol_info):
        self.vol_id = vol_id
        self.vol_info = vol_info


class Closed(errors.Base):

    msg = "Operation on closed database connection"


class InvalidDatabase(errors.Base):

    msg = "Invalid database: {self.reason}"

    def __init__(self, reason):
        self.reason = reason


def create_db():
    return _DB.create()


def version_info():
    db = open()
    with closing(db):
        return db.version_info()


def open():
    return _DB()


# Private

log = logging.getLogger("storage.managevolumedb")


class _closed_env(object):

    def __getattr__(self, name):
        raise Closed


_CLOSED_ENV = _closed_env()


class _DB(object):

    # LMDB database must be opened exactly once per process. Opening another
    # instace will break file locks when closing it.
    _env = _CLOSED_ENV
    _users = 0
    _lock = threading.Lock()

    @classmethod
    def _open_env(cls, create=False):
        with cls._lock:
            if cls._env is _CLOSED_ENV:
                cls._env = lmdb.open(
                    DB_FILE,
                    map_size=MAP_SIZE,
                    max_dbs=MAX_DBS,
                    create=create)
            cls._users += 1

    @classmethod
    def _close_env(cls):
        with cls._lock:
            if cls._users == 0:
                raise RuntimeError("Env is not used by anyone")

            cls._users -= 1
            if cls._users == 0:
                cls._env.close()
                cls._env = _CLOSED_ENV

    @classmethod
    def create(cls):
        """
        Create the database files.
        """
        cls._open_env(create=True)
        try:
            cls._env.open_db(VOLUMES_DB, create=True)
            cls._env.open_db(MULTIPATHS_DB, create=True)
            versions = cls._env.open_db(VERSIONS_DB, create=True)

            with cls._env.begin(write=True) as txn:
                updated = datetime.datetime.utcnow().replace(microsecond=0)
                info = {
                    "version": VERSION,
                    "description": "Initial version",
                    "updated": str(updated),
                }
                # Ensure sorting up to 32 bit value.
                key = b"%010d" % VERSION
                data = json.dumps(info).encode()
                txn.put(key, data, db=versions)
        finally:
            cls._close_env()

    def __init__(self):
        self._open_env(create=False)

    def close(self):
        if self._env is not _CLOSED_ENV:
            self._env = _CLOSED_ENV
            self._close_env()

    def get_volume(self, vol_id):
        """
        Return info stored for volume vol_id.
        """
        vol_id = vol_id.encode()
        volumes = self._env.open_db(VOLUMES_DB)
        with self._env.begin() as txn:
            vol_data = txn.get(vol_id, db=volumes)
            if vol_data is None:
                raise NotFound(vol_id)

            return json.loads(vol_data)

    def add_volume(self, vol_id, connection_info):
        """
        Add volume vol_id to database.
        """
        log.info("Adding volume %s connection_info=%s",
                 vol_id, connection_info)

        vol_id = vol_id.encode()
        volumes = self._env.open_db(VOLUMES_DB)

        with self._env.begin(write=True) as txn:
            vol_data = txn.get(vol_id, db=volumes)
            if vol_data is not None:
                vol_info = json.loads(vol_data)
                raise VolumeAlreadyExists(vol_id, vol_info)

            vol_info = {"connection_info": connection_info}
            vol_data = json.dumps(vol_info).encode("utf-8")
            txn.put(vol_id, vol_data, db=volumes)

    def update_volume(self, vol_id, path, attachment, multipath_id):
        """
        Add volume vol_id info.
        """
        log.info("Updating volume %s path=%s, attachment=%s, multipath_id=%s",
                 vol_id, path, attachment, multipath_id)

        vol_id = vol_id.encode()
        volumes = self._env.open_db(VOLUMES_DB)
        multipaths = self._env.open_db(MULTIPATHS_DB)

        with self._env.begin(write=True) as txn:
            vol_data = txn.get(vol_id, db=volumes)
            if vol_data is None:
                raise NotFound(vol_id)

            vol_info = json.loads(vol_data)

            vol_info["path"] = path
            vol_info["attachment"] = attachment
            if multipath_id:
                vol_info["multipath_id"] = multipath_id

            vol_data = json.dumps(vol_info).encode()
            txn.put(vol_id, vol_data, db=volumes)

            if multipath_id:
                txn.put(multipath_id.encode(), vol_id, db=multipaths)

    def remove_volume(self, vol_id):
        """
        Remove volume vol_id from database.
        """
        log.info("Removing volume %s", vol_id)

        vol_id = vol_id.encode()
        volumes = self._env.open_db(VOLUMES_DB)
        multipaths = self._env.open_db(MULTIPATHS_DB)

        with self._env.begin(write=True) as txn:
            vol_data = txn.get(vol_id, db=volumes)
            if vol_data is None:
                raise NotFound(vol_id)

            vol_info = json.loads(vol_data)
            multipath_id = vol_info.get("multipath_id")
            if multipath_id:
                txn.delete(multipath_id.encode(), db=volumes)

            txn.delete(vol_id, db=volumes)

    def version_info(self):
        """
        Return database version info.
        """
        versions = self._env.open_db(VERSIONS_DB)
        with self._env.begin() as txn:
            cur = txn.cursor(versions)
            if not cur.last():
                raise InvalidDatabase("Database version not found")

            data = cur.value()
            return json.loads(data)

    def owns_multipath(self, multipath_id):
        """
        Return True if multipath device is owned by a managed volume.
        """
        multipaths = self._env.open_db(MULTIPATHS_DB)
        with self._env.begin() as txn:
            cur = txn.cursor(multipaths)
            return cur.set_key(multipath_id.encode())
