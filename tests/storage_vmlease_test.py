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

from __future__ import absolute_import
from __future__ import print_function

import io
import os
import time
import timeit

from contextlib import contextmanager

from testlib import VdsmTestCase
from testlib import namedTemporaryDir
from testlib import make_uuid

from vdsm.storage import vmlease


class TestIndex(VdsmTestCase):

    def test_format(self):
        with make_index() as index:
            self.assertEqual(index.leases(), {})

    def test_lookup_missing(self):
        with make_index() as index:
            with self.assertRaises(vmlease.NoSuchLease):
                index.lookup(make_uuid())

    def test_add(self):
        with make_index() as index:
            lease_id = make_uuid()
            start_time = int(time.time())
            lease_info = index.add(lease_id)
        self.assertEqual(lease_info.lockspace, index.lockspace)
        self.assertEqual(lease_info.resource, lease_id)
        self.assertEqual(lease_info.path, index.path)
        self.assertTrue(start_time <= lease_info.modified <= start_time + 1)

    def test_leases(self):
        with make_index() as index:
            leases = [make_uuid() for i in range(3)]
            for lease in leases:
                index.add(lease)
            expected = {
                leases[0]: vmlease.LEASE_BASE,
                leases[1]: vmlease.LEASE_BASE + vmlease.LEASE_SIZE,
                leases[2]: vmlease.LEASE_BASE + vmlease.LEASE_SIZE * 2,
            }
            self.assertEqual(index.leases(), expected)

    def test_add_exists(self):
        with make_index() as index:
            lease_id = make_uuid()
            index.add(lease_id)
            with self.assertRaises(vmlease.LeaseExists):
                index.add(lease_id)

    def test_lookup_exists(self):
        with make_index() as index:
            lease_id = make_uuid()
            add_info = index.add(lease_id)
            lookup_info = index.lookup(lease_id)
            self.assertEqual(add_info, lookup_info)

    def test_remove_exists(self):
        with make_index() as index:
            leases = [make_uuid() for i in range(3)]
            for lease in leases:
                index.add(lease)
            index.remove(leases[1])
            expected = {
                leases[0]: vmlease.LEASE_BASE,
                leases[2]: vmlease.LEASE_BASE + vmlease.LEASE_SIZE * 2,
            }
            self.assertEqual(index.leases(), expected)

    def test_remove_missing(self):
        with make_index() as index:
            lease_id = make_uuid()
            with self.assertRaises(vmlease.NoSuchLease):
                index.remove(lease_id)

    def test_add_first_free_slot(self):
        with make_index() as index:
            leases = [make_uuid() for i in range(4)]
            for lease in leases[:3]:
                index.add(lease)
            index.remove(leases[1])
            index.add(leases[3])
            expected = {
                leases[0]: vmlease.LEASE_BASE,
                leases[3]: vmlease.LEASE_BASE + vmlease.LEASE_SIZE,
                leases[2]: vmlease.LEASE_BASE + vmlease.LEASE_SIZE * 2,
            }
            self.assertEqual(index.leases(), expected)

    def test_time_lookup(self):
        setup = """
import os
from testlib import make_uuid
from vdsm.storage import vmlease

path = "%s"
lockspace = os.path.basename(os.path.dirname(path))
lease_id = make_uuid()

def bench():
    with vmlease.Index(lockspace, path) as index:
        try:
            index.lookup(lease_id)
        except vmlease.NoSuchLease:
            pass
"""
        with make_index() as index:
            count = 1000
            elapsed = timeit.timeit("bench()", setup=setup % index.path,
                                    number=count)
            print("%d lookups in %.6f seconds (%.6f seconds per lookup)"
                  % (count, elapsed, elapsed / count))

    def test_time_add(self):
        setup = """
import os
from testlib import make_uuid
from vdsm.storage import vmlease

path = "%s"
lockspace = os.path.basename(os.path.dirname(path))

def bench():
    lease_id = make_uuid()
    with vmlease.Index(lockspace, path) as index:
        index.add(lease_id)
"""
        with make_index() as index:
            count = 100
            elapsed = timeit.timeit("bench()", setup=setup % index.path,
                                    number=count)
            print("%d adds in %.6f seconds (%.6f seconds per add)"
                  % (count, elapsed, elapsed / count))


@contextmanager
def make_index():
    with namedTemporaryDir() as tmpdir:
        lockspace = os.path.basename(tmpdir)
        path = os.path.join(tmpdir, "vmleases")
        with io.open(path, "wb") as f:
            f.seek(vmlease.INDEX_SIZE - 1)
            f.write(b"\0")
        with vmlease.Index(lockspace, path) as index:
            index.format()
            yield index
