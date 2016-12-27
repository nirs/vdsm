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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301 USA
#
# Refer to the README and COPYING files for full details of the license
#

from __future__ import absolute_import
import argparse

from vdsm import utils
from vdsm.storage import xlease
from . import expose


@expose('format-xleases')
def format_xleases(*args):
    """
    format-xleases sd_id path

    This is a destructive operation, all the leases on this volume will be
    deleted. This must not be used on an active storage domain.

    The caller is responsible to activate the lv on block storage before
    calling this and deactivating the lv afterwards.

    If this fails, the volume will not be usable (it will be marked as
    "updating"), but the operation can be tried again.

    Creating xleases volume on file storage:

        PATH=/rhev/data-center/mnt/server:_export/sd_id/dom_md/xleases
        truncate -s 1G $PATH
        vdsm-tool format-xleases sd_id $PATH

    Creating the xleases volume on block storage:

        lvcreate --name xleases --size 1g sd_id
        vdsm-tool format-xleases sd_id /dev/sd_id/xleases
        lvchange -an sd_id/xleases
    """
    args = parse_args(args)
    backend = xlease.DirectFile(args.path)
    with utils.closing(backend):
        xlease.format_index(args.sd_id, backend)


@expose('rebuild-xleases')
def rebuild_xleases(*args):
    """
    rebuild-xleases sd_id path

    This operation synchronize the xleases index with the actual leases on
    storage. This must not be used on an active storage domain.

    The caller is responsible to activate the lv on block storage before
    calling this and deactivating the lv afterwards.

    If this fails, the volume will not be usable (it will be marked as
    "updating"), but the operation can be tried again.

    Rebuilding xleases volume on file storage:

        PATH=/rhev/data-center/mnt/server:_export/sd_id/dom_md/xleases
        vdsm-tool rebuild-xleases sd_id $PATH

    Rebuilding the xleases volume on block storage:

        lvchange -ay sd_id/xleases
        vdsm-tool rebuild-xleases sd_id /dev/sd_id/xleases
        lvchange -an sd_id/xleases
    """
    args = parse_args(args)
    backend = xlease.DirectFile(args.path)
    with utils.closing(backend):
        xlease.rebuild_index(args.sd_id, backend)


def parse_args(args):
    parser = argparse.ArgumentParser()
    parser.add_argument('sd_id', help="storage domain UUID")
    parser.add_argument('path', help="path to xleases volume")
    return parser.parse_args(args[1:])
