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
# Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301 USA
#
# Refer to the README and COPYING files for full details of the license
#
"""
To run this test you must run the tests as root, or have writable /run/vdsm and
running supervdsm serving the user running the tests.

To setup the environment for unprivileged user:

    $ sudo mkdir /run/vdsm

    $ sudo chown $USER:$USER /run/vdsm

    $ sudo env PYTHONPATH=lib static/usr/sbin/supervdsmd \
          --data-center /var/tmp/vdsm/data-center \
          --sockfile /run/vdsm/svdsm.sock \
          --user=$USER \
          --group=$USER \
          --logger-conf tests/conf/svdsm.logger.conf \
          --disable-gluster \
          --disable-network
"""

from __future__ import absolute_import
from __future__ import division

import os

import pytest

from vdsm import test
from vdsm.common import supervdsm

requires_privileges = pytest.mark.skipif(
    not (os.geteuid() == 0 or supervdsm.is_accessible()),
    reason="requires root or running supervdsm")


@requires_privileges
def test_ping():
    assert test.ping()


@requires_privileges
def test_echo():
    args = (1, 2, 3)
    kwargs = {"a": 4, "b": 5}
    assert test.echo(*args, **kwargs) == (args, kwargs)


@requires_privileges
def test_whoami():
    assert test.whoami() == "root"
