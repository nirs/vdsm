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
lease - manage lease devices in the virt subsystem.

Overview
--------

Vdsm storage subsystem is creating sanlock leases (paxos leases) on
shared storage. To acquire a lease for a vm, we add a "lease" device to
the domain xml.  Libvirt would connect to sanlock, acquire the lease,
and then pass the sanlock fd to qemu. When qemu terminates, sanlock
detects that the fd was closed, and release the lease.

Sanlock is ensuring that if a host is connected to storage, and the
owner of the lease is running, the lease cannot be acquired on another
host. If the host cannot access storage, sanlock will kill the owner of
the lease, so the lease can be acquired from another host.

Lease types
-----------

We have two kinds of leases:

1. Volume leases - each volume has a lease area on storage. If a drive
   is with shared="exclusive", we add a lease device for the drive top
   volume to the domain xml. The lease information is returned when
   preparing an image using `Image.prepare` verb.

2. VM leases - each vm may have a lease area on storage, accesible using
   the VM uuid. Engine send a lease device spec with partial
   information, and the missing details are fetched using `Lease.info`
   verb.

Drive with volume leases do not support live snaphost, hotplug/unplug,
or live storage migration. In this case we need to unplug the lease
device for the old volume, and plug a new lease device for the new
volume. This is not implemented yet, and instead we just block these
operation if a drive has a volume lease.

VM lease is implemented using external leases, which are more generic
than drive leases; actually drive leases can be implemented using
external leases. When using VM lease, live snapshot, hotplug/unplug and
live storge migration (should) work.

From libvirt point of view, there is no difference between drive leases
or vm lease, all are lease devices.

APIs in this module
-------------------

- `lease.CannotPrepare` - raised if preparing a lease fails. This is a
  fatal error failing to start a vm.

- `lease.MissingArgument` - raised if required argument is missing when
  creating a `lease.Device`. This is a fatal error failing to start a
  vm.

- `lease.Device` - a libvirt lease device, creating the xml for the
  domain xml.

- `lease.prepare` - prepare lease devices information so a
  `lease.Device` can be created.

See also
--------

- Libvirt documentation
  https://libvirt.org/formatdomain.html#elementsLease

- Virtual machine lock manager, sanlock plugin
  https://libvirt.org/locking-sanlock.html

- VM leases feature page
  http://www.ovirt.org/develop/release-management/features/storage/vm-leases/

- External leases stoage module
  `vdsm.storage.xlease`

"""

import logging

from vdsm.common import response

from .. import vmxml
from . import core

log = logging.getLogger("virt.lease")


class Error(Exception):
    def __str__(self):
        return self.msg.format(self=self)


class CannotPrepare(Error):
    msg = "Error preparing lease device {self.device}: {self.reason}"

    def __init__(self, device, reason):
        self.device = device
        self.reason = reason


class MissingArgument(Error):
    msg = "Missing required argument {self.missing} in {self.available}"

    def __init__(self, missing, available):
        self.missing = missing
        self.available = available


class Device(core.Base):
    """
    VM lease device.
    """
    __slots__ = ("lease_id", "sd_id", "path", "offset")

    @classmethod
    def update_device_info(cls, vm, device_conf):
        """
        We don't have anything to update yet. Keeping this interface so
        other code do not need to know that.
        """

    def __init__(self, conf, log, **kwargs):
        """
        Initialize a lease element.

        :param uuid lease_id: Lease id, e.g. volume id for a volume lease, or
            vm id for a vm lease
        :param uuid sd_id: Storage domain uuid where lease file is located
        :param str path: Path to lease file or block device
        :param int offset: Offset in lease file in bytes
        """
        # TODO: should be solved for all devices
        for key in self.__slots__:
            if key not in kwargs:
                raise MissingArgument(key, kwargs)
        super(Device, self).__init__(conf, log, **kwargs)

    def getXML(self):
        """
        Return xml element.

        <lease>
            <key>12523e3d-ad22-410c-8977-d2a7bf458a65</key>
            <lockspace>c2a6d7c8-8d81-4e01-9ed4-7eb670713448</lockspace>
            <target offset="1048576"
                    path="/dev/c2a6d7c8-8d81-4e01-9ed4-7eb670713448/leases"/>
        </lease>

        :rtype: `vmxml.Element`
        """
        lease = vmxml.Element('lease')
        lease.appendChildWithArgs('key', text=self.lease_id)
        lease.appendChildWithArgs('lockspace', text=self.sd_id)
        lease.appendChildWithArgs('target', path=self.path,
                                  offset=str(self.offset))
        return lease

    def __repr__(self):
        return ("<lease.Device sd_id={self.sd_id}, "
                "lease_id={self.lease_id}, "
                "path={self.path}, "
                "offset={self.offset} "
                "at {addr:#x}>").format(self=self, addr=id(self))


def prepare(storage, devices):
    """
    Add lease path and offset to devices with partial information.

    Engine send only the lease id and storage domain id. Using both ids
    we can fetch the lease path and the offset from storage. After
    leases devices are prepared, they are persisted, so we don't need to
    access storage again.

    This operation does not change storage so it does not need any
    cleanup.

    :param storage: Implemeent lease_info api.
    :param iterable devices: Iterable of lease devices dicts with
        partial lease information.
    :raises `CannotPrepare`: if one of the leases could not be prepared.
    """
    for device in devices:
        if _is_prepared(device):
            log.debug("Using prepared lease device: %s", device)
        else:
            _prepare_device(storage, device)
            log.debug("Prepared lease device: %s", device)


def _prepare_device(storage, device):
    lease = dict(sd_id=device["sd_id"], lease_id=device["lease_id"])
    res = storage.lease_info(lease)
    if response.is_error(res):
        raise CannotPrepare(device, res["status"]["message"])
    lease_info = res["result"]
    device["path"] = lease_info["path"]
    device["offset"] = lease_info["offset"]


def _is_prepared(device):
    return "path" in device and "offset" in device
