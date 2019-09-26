#
# Copyright 2011-2017 Red Hat, Inc.
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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston,
# MA 02110-1301 USA
#
# Refer to the README and COPYING files for full details of the license
#

from __future__ import absolute_import
from __future__ import division

import functools
import os
from multiprocessing.managers import BaseManager, RemoteError
import logging
import threading

from vdsm.common import constants
from vdsm.common import function
from vdsm.common.panic import panic

_g_singletonSupervdsmInstance = None
_g_singletonSupervdsmInstance_lock = threading.Lock()


ADDRESS = os.path.join(constants.P_VDSM_RUN, "svdsm.sock")


class _SuperVdsmManager(BaseManager):
    pass


class ProxyCaller(object):

    def __init__(self, supervdsmProxy, funcName):
        self._funcName = funcName
        self._supervdsmProxy = supervdsmProxy

    def __call__(self, *args, **kwargs):
        callMethod = lambda: \
            getattr(self._supervdsmProxy._svdsm, self._funcName)(*args,
                                                                 **kwargs)
        try:
            return callMethod()
        except RemoteError:
            self._supervdsmProxy._connect()
            raise RuntimeError(
                "Broken communication with supervdsm. Failed call to %s"
                % self._funcName)


class SuperVdsmProxy(object):
    """
    A wrapper around all the supervdsm init stuff
    """
    _log = logging.getLogger("SuperVdsmProxy")

    def __init__(self):
        self._manager = None
        self._svdsm = None
        self._connect()

    def open(self, *args, **kwargs):
        # pylint: disable=no-member
        return self._manager.open(*args, **kwargs)

    def _connect(self):
        self._manager = _SuperVdsmManager(address=ADDRESS, authkey=b'')
        self._manager.register('instance')
        self._manager.register('open')
        self._log.debug("Trying to connect to Super Vdsm")
        try:
            function.retry(
                self._manager.connect, Exception, timeout=60, tries=3)
        except Exception as ex:
            msg = "Connect to supervdsm service failed: %s" % ex
            panic(msg)

        # pylint: disable=no-member
        self._svdsm = self._manager.instance()

    def __getattr__(self, name):
        return ProxyCaller(self, name)


def getProxy():
    global _g_singletonSupervdsmInstance
    if _g_singletonSupervdsmInstance is None:
        with _g_singletonSupervdsmInstance_lock:
            if _g_singletonSupervdsmInstance is None:
                _g_singletonSupervdsmInstance = SuperVdsmProxy()
    return _g_singletonSupervdsmInstance


def run_as_root(func):
    """
    Decorate a function to make it run via supervdsm.

    Assumes that the supervdsm interface is modulename_funcname. You need to
    provide a supervdsm entry point in lib/vdsm/supervdsm_api/modulename.py.
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        if os.geteuid() == 0:
            return func(*args, **kwargs)

        module_name = func.__module__.split(".")[-1]
        full_name = "{}_{}".format(module_name, func.__name__)
        proxy_call = getattr(getProxy(), full_name)
        return proxy_call(*args, **kwargs)

    return wrapper


def is_accessible():
    """
    Return True if supervdsm socket is accessible.
    """
    return os.access(ADDRESS, os.W_OK)
