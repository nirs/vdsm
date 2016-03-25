#
# Copyright 2012-2016 Red Hat, Inc.
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

import re

from collections import namedtuple
from threading import Lock

from vdsm import constants
from vdsm.utils import AsyncProcessOperation

import misc

# iscsiadm exit statuses
ISCSI_ERR_SESS_EXISTS = 15
ISCSI_ERR_LOGIN_AUTH_FAILED = 24
ISCSI_ERR_OBJECT_NOT_FOUND = 21

Iface = namedtuple('Iface', 'ifacename transport_name hwaddress ipaddress \
                    net_ifacename initiatorname')


class IscsiError(RuntimeError):
    pass


class ReservedInterfaceNameError(IscsiError):
    pass


class IscsiInterfaceError(IscsiError):
    pass


class IsciInterfaceAlreadyExistsError(IscsiInterfaceError):
    pass


class IsciInterfaceCreationError(IscsiInterfaceError):
    pass


class IscsiInterfaceDoesNotExistError(IscsiInterfaceError):
    pass


class IscsiInterfaceUpdateError(IscsiInterfaceError):
    pass


class IscsiInterfaceDeletionError(IscsiInterfaceError):
    pass


class IscsiDiscoverdbError(IscsiError):
    pass


class IscsiInterfaceListingError(IscsiError):
    pass


class IscsiAuthenticationError(IscsiError):
    pass


class IscsiNodeError(IscsiError):
    pass


class IscsiSessionNotFound(IscsiError):
    pass


class IscsiSessionError(IscsiError):
    pass

_RESERVED_INTERFACES = ("default", "tcp", "iser")

# Running multiple iscsiadm commands in parallel causes random problems.
# This serializes all calls to iscsiadm.
# Remove when iscsid is actually thread safe.
_iscsiadmLock = Lock()


def _runCmd(args, hideValue=False, sync=True):
    # FIXME: I don't use supervdsm because this entire module has to just be
    # run as root and there is no such feature yet in supervdsm. When such
    # feature exists please change this.
    with _iscsiadmLock:
        cmd = [constants.EXT_ISCSIADM] + args

        printCmd = None
        if hideValue:
            printCmd = cmd[:]
            for i, arg in enumerate(printCmd):
                if arg != "-v":
                    continue

                if i < (len(printCmd) - 1):
                    printCmd[i + 1] = "****"

        return misc.execCmd(cmd, printable=printCmd, sudo=True, sync=sync)


def iface_exists(interfaceName):
    # FIXME: can be optimized by checking /var/lib/iscsi/ifaces
    for iface in iface_list():
        if interfaceName == iface.ifacename:
            return True

    return False


def iface_new(name):
    if name in _RESERVED_INTERFACES:
        raise ReservedInterfaceNameError(name)

    rc, out, err = _runCmd(["-m", "iface", "-I", name, "--op=new"])
    if rc == 0:
        return

    if iface_exists(name):
        raise IsciInterfaceAlreadyExistsError(name)

    raise IsciInterfaceCreationError(name, rc, out, err)


def iface_update(name, key, value):
    rc, out, err = _runCmd(["-m", "iface", "-I", name, "-n", key, "-v", value,
                            "--op=update"])
    if rc == 0:
        return

    if not iface_exists(name):
        raise IscsiInterfaceDoesNotExistError(name)

    raise IscsiInterfaceUpdateError(name, rc, out, err)


def iface_delete(name):
    rc, out, err = _runCmd(["-m", "iface", "-I", name, "--op=delete"])
    if rc == 0:
        return

    if not iface_exists(name):
        raise IscsiInterfaceDoesNotExistError(name)

    raise IscsiInterfaceDeletionError(name)


def iface_list(out=None):
    # FIXME: This can be done more efficiently by iterating
    # /var/lib/iscsi/ifaces. Fix if ever a performance bottleneck.
    # "iscsiadm -m iface" output format:
    #   <iscsi_ifacename> <transport_name>,<hwaddress>,<ipaddress>,\
    #   <net_ifacename>,<initiatorname>
    if out is None:
        rc, out, err = _runCmd(["-m", "iface"])
        if rc != 0:
            raise IscsiInterfaceListingError(rc, out, err)

    for line in out:
        yield Iface._make(None if value == '<empty>' else value
                          for value in re.split('[\s,]', line))


def iface_info(name):
    # FIXME: This can be done more effciently by reading
    # /var/lib/iscsi/ifaces/<iface name>. Fix if ever a performance bottleneck.
    rc, out, err = _runCmd(["-m", "iface", "-I", name])
    if rc == 0:
        res = {}
        for line in out:
            if line.startswith("#"):
                continue

            key, value = line.split("=", 1)

            if value.strip() == '<empty>':
                continue

            res[key.strip()] = value.strip()

        return res

    if not iface_exists(name):
        raise IscsiInterfaceDoesNotExistError(name)

    raise IscsiInterfaceListingError(rc, out, err)


def discoverydb_new(discoveryType, iface, portal):
    rc, out, err = _runCmd(["-m", "discoverydb", "-t", discoveryType, "-I",
                            iface, "-p", portal, "--op=new"])
    if rc == 0:
        return

    if not iface_exists(iface):
        raise IscsiInterfaceDoesNotExistError(iface)

    raise IscsiDiscoverdbError(rc, out, err)


def discoverydb_update(discoveryType, iface, portal, key, value,
                       hideValue=False):
    rc, out, err = _runCmd(["-m", "discoverydb", "-t", discoveryType, "-I",
                            iface, "-p", portal, "-n", key, "-v", value,
                            "--op=update"],
                           hideValue)
    if rc == 0:
        return

    if not iface_exists(iface):
        raise IscsiInterfaceDoesNotExistError(iface)

    raise IscsiDiscoverdbError(rc, out, err)


def discoverydb_discover(discoveryType, iface, portal):
    rc, out, err = _runCmd(["-m", "discoverydb", "-t", discoveryType, "-I",
                            iface, "-p", portal, "--discover"])
    if rc == 0:
        res = []
        for line in out:
            if line.startswith("["):  # skip IPv6 targets
                continue
            rest, iqn = line.split()
            rest, tpgt = rest.split(",")
            ip, port = rest.split(":")
            res.append((ip, int(port), int(tpgt), iqn))

        return res

    if not iface_exists(iface):
        raise IscsiInterfaceDoesNotExistError(iface)

    if rc == ISCSI_ERR_LOGIN_AUTH_FAILED:
        raise IscsiAuthenticationError(rc, out, err)

    raise IscsiDiscoverdbError(rc, out, err)


def discoverydb_delete(discoveryType, iface, portal):
    rc, out, err = _runCmd(["-m", "discoverydb", "-t", discoveryType, "-I",
                            iface, "-p", portal, "--op=delete"])
    if rc == 0:
        return

    if not iface_exists(iface):
        raise IscsiInterfaceDoesNotExistError(iface)

    raise IscsiDiscoverdbError(rc, out, err)


def node_new(iface, portal, targetName):
    rc, out, err = _runCmd(["-m", "node", "-T", targetName, "-I", iface, "-p",
                            portal, "--op=new"])
    if rc == 0:
        return

    if not iface_exists(iface):
        raise IscsiInterfaceDoesNotExistError(iface)

    raise IscsiNodeError(rc, out, err)


def node_update(iface, portal, targetName, key, value, hideValue=False):
    rc, out, err = _runCmd(["-m", "node", "-T", targetName, "-I", iface, "-p",
                            portal, "-n", key, "-v", value, "--op=update"],
                           hideValue)
    if rc == 0:
        return

    if not iface_exists(iface):
        raise IscsiInterfaceDoesNotExistError(iface)

    raise IscsiNodeError(rc, out, err)


def node_delete(iface, portal, targetName):
    rc, out, err = _runCmd(["-m", "node", "-T", targetName, "-I", iface, "-p",
                            portal, "--op=delete"])
    if rc == 0:
        return

    if not iface_exists(iface):
        raise IscsiInterfaceDoesNotExistError(iface)

    raise IscsiNodeError(rc, out, err)


def node_disconnect(iface, portal, targetName):
    rc, out, err = _runCmd(["-m", "node", "-T", targetName, "-I", iface, "-p",
                            portal, "-u"])
    if rc == 0:
        return

    if not iface_exists(iface):
        raise IscsiInterfaceDoesNotExistError(iface)

    if rc == ISCSI_ERR_OBJECT_NOT_FOUND:
        raise IscsiSessionNotFound(iface, portal, targetName)

    raise IscsiNodeError(rc, out, err)


def node_login(iface, portal, targetName):
    rc, out, err = _runCmd(["-m", "node", "-T", targetName, "-I", iface, "-p",
                            portal, "-l"])
    if rc == 0:
        return

    if not iface_exists(iface):
        raise IscsiInterfaceDoesNotExistError(iface)

    if rc == ISCSI_ERR_LOGIN_AUTH_FAILED:
        raise IscsiAuthenticationError(rc, out, err)

    raise IscsiNodeError(rc, out, err)


def session_rescan_async():
    proc = _runCmd(["-m", "session", "-R"], sync=False)

    def parse_result(rc, out, err):
        if rc == 0:
            return

        raise IscsiSessionError(rc, out, err)

    return AsyncProcessOperation(proc, parse_result)


def session_rescan():
    aop = session_rescan_async()
    return aop.result()


def session_logout(sessionId):
    rc, out, err = _runCmd(["-m", "session", "-r", str(sessionId), "-u"])
    if rc == 0:
        return

    raise IscsiSessionError(rc, out, err)
