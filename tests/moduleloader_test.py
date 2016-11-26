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

import importlib
import os
import sys
from vdsm import moduleloader
from vdsm import utils
from testlib import permutations, expandPermutations, namedTemporaryDir
from testlib import forked
from testlib import VdsmTestCase as TestCaseBase


@expandPermutations
class ImportModulesTest(TestCaseBase):

    @permutations(
        [(('a.py', 'b.py'), ('a', 'b')),
         (('a.py', 'b.py', 'a.pyioas'), ('a', 'b')),
         (('a.py', 'b.py', '_my.py'), ('a', 'b', '_my'))]
    )
    @forked
    def test_import_modules(self, files, expected_modules):
        with namedTemporaryDir() as path:
            for f in files:
                utils.touchFile(os.path.join(path, f))
            utils.touchFile(os.path.join(path, '__init__.py'))
            sys.path.insert(0, os.path.dirname(path))
            pkg = importlib.import_module(os.path.basename(path))
            result = moduleloader.load_modules(pkg)

        result = frozenset(result.keys())
        expected = frozenset(expected_modules)
        self.assertEqual(result, expected)
