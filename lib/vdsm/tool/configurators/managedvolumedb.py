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
from contextlib import closing

from vdsm import constants
from vdsm.common import errors
from vdsm.storage import fileUtils
from vdsm.storage import managedvolumedb as mvdb

from . import YES, NO

log = logging.getLogger("tool.configurators.managevolumedb")


class DatabaseError(errors.Base):

    msg = "Managed volumes database error, see log for details"


def configure():
    """
    Create database for managed volumes
    """
    if not db_exists():
        sys.stdout.write("Creating managed volume database\n")
        mvdb.create_db()
        set_db_ownership()
    else:
        if not db_owned_by_vdsm():
            sys.stdout.write("Fixing managed volume database ownership\n")
            set_db_ownership()

        if not db_version_ok():
            raise DatabaseError("Unexpeced database version")


def isconfigured():
    """
    Return YES if managedvolumedb is configured, otherwise NO.
    """
    if db_exists() and db_owned_by_vdsm() and db_version_ok():
        sys.stdout.write("Managed volume database configured\n")
        return YES
    else:
        sys.stdout.write("Managed volume database needs configuration\n")
        return NO


def db_exists():
    return os.path.isfile(mvdb.DB_FILE)


def db_owned_by_vdsm():
    st = os.stat(mvdb.DB_FILE)
    return (st.st_uid == fileUtils.resolveUid(constants.VDSM_USER) and
            st.st_gid == fileUtils.resolveGid(constants.VDSM_GROUP))


def set_db_ownership():
    fileUtils.chown(mvdb.DB_FILE, constants.VDSM_USER, constants.VDSM_GROUP)


def db_version_ok():
    version = mvdb.version_info()
    log.debug("Database version %s", version["version"])
    return version["version"] == mvdb.VERSION
