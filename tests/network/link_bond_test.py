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

from contextlib import contextmanager
import os

import ctypes

from nose.plugins.attrib import attr

from testlib import VdsmTestCase as TestCaseBase, mock

from .nettestlib import dummy_devices, check_sysfs_bond_permission
from testValidation import broken_on_ci

from vdsm.network.link import iface
from vdsm.network.link.bond import Bond
from vdsm.network.link.bond import sysfs_options
from vdsm.utils import random_iface_name


def setup_module():
    check_sysfs_bond_permission()


@attr(type='integration')
# TODO: We should get rid of this patch in future patches and replace it with a
# stream that we update for each run (scanning the bond modes).
@mock.patch.object(sysfs_options, 'BONDING_DEFAULTS',
                   sysfs_options.BONDING_DEFAULTS
                   if os.path.exists(sysfs_options.BONDING_DEFAULTS)
                   else '../static/usr/share/vdsm/bonding-defaults.json')
class LinkBondTests(TestCaseBase):

    @broken_on_ci("fails on jenkins", exception=ctypes.ArgumentError)
    def test_bond_without_slaves(self):
        with bond_device() as bond:
            self.assertFalse(iface.is_up(bond.master))

    @broken_on_ci("fails on jenkins", exception=ctypes.ArgumentError)
    def test_bond_with_slaves(self):
        with dummy_devices(2) as (nic1, nic2):
            with bond_device() as bond:
                bond.add_slaves((nic1, nic2))
                self.assertFalse(iface.is_up(bond.master))

    @broken_on_ci("fails on jenkins", exception=ctypes.ArgumentError)
    def test_bond_devices_are_up(self):
        with dummy_devices(2) as (nic1, nic2):
            with bond_device() as bond:
                bond.add_slaves((nic1, nic2))
                bond.up()
                self.assertTrue(iface.is_up(nic1))
                self.assertTrue(iface.is_up(nic2))
                self.assertTrue(iface.is_up(bond.master))

    @broken_on_ci("fails on jenkins", exception=ctypes.ArgumentError)
    def test_bond_exists(self):
        OPTIONS = {'mode': '1', 'miimon': '300'}
        with dummy_devices(2) as (nic1, nic2):
            with bond_device() as _bond:
                _bond.set_options(OPTIONS)
                _bond.add_slaves((nic1, nic2))
                _bond.up()

                bond = Bond(_bond.master)
                self.assertEqual(bond.slaves, set((nic1, nic2)))
                self.assertEqual(bond.options, OPTIONS)

    def test_bond_list(self):
        with bond_device() as b1, bond_device() as b2, bond_device() as b3:
            actual_bond_set = set(Bond.bonds())
            expected_bond_set = set([b1.master, b2.master, b3.master])
            self.assertLessEqual(expected_bond_set, actual_bond_set)

    @broken_on_ci("fails on jenkins", exception=ctypes.ArgumentError)
    def test_bond_create_failure_on_slave_add(self):
        with dummy_devices(2) as (nic1, nic2):
            with bond_device() as base_bond:
                base_bond.add_slaves((nic1, nic2))

                bond_name = random_iface_name('bond_', max_length=11)
                with self.assertRaises(IOError):
                    with Bond(bond_name) as broken_bond:
                        broken_bond.create()
                        broken_bond.add_slaves((nic1, nic2))
                self.assertFalse(Bond(bond_name).exists())

    @broken_on_ci("fails on jenkins", exception=ctypes.ArgumentError)
    def test_bond_edit_failure_on_slave_add(self):
        with dummy_devices(2) as (nic1, nic2):
            with bond_device() as base_bond, bond_device() as edit_bond:
                base_bond.add_slaves((nic1,))
                edit_bond.add_slaves((nic2,))

                with self.assertRaises(IOError):
                    with Bond(edit_bond.master) as broken_bond:
                        self.assertTrue(broken_bond.exists())
                        broken_bond.add_slaves((nic1,))
                self.assertTrue(edit_bond.exists())
                self.assertEqual(set((nic2,)), edit_bond.slaves)

    @broken_on_ci("fails on jenkins", exception=ctypes.ArgumentError)
    def test_bond_set_options(self):
        OPTIONS = {'mode': '1', 'miimon': '300'}

        with dummy_devices(2) as (nic1, nic2):
            with bond_device() as bond:
                bond.set_options(OPTIONS)
                bond.add_slaves((nic1, nic2))
                bond.up()

                _bond = Bond(bond.master)
                self.assertEqual(_bond.options, OPTIONS)

    @broken_on_ci("fails on jenkins", exception=ctypes.ArgumentError)
    def test_bond_edit_options(self):
        OPTIONS_A = {'mode': '1', 'miimon': '300'}
        OPTIONS_B = {'mode': '2'}
        OPTIONS_C = {'mode': '2', 'miimon': '150'}

        with dummy_devices(2) as (nic1, nic2):
            with bond_device() as bond:
                bond.set_options(OPTIONS_A)
                bond.add_slaves((nic1, nic2))
                _bond = Bond(bond.master)
                self.assertEqual(_bond.options, OPTIONS_A)

                bond.set_options(OPTIONS_B)
                _bond.refresh()
                self.assertEqual(_bond.options, OPTIONS_B)

                bond.set_options(OPTIONS_C)
                _bond.refresh()
                self.assertEqual(_bond.options, OPTIONS_C)


@attr(type='integration')
# TODO: We should get rid of this patch in future patches and replace it with a
# stream that we update for each run (scanning the bond modes).
@mock.patch.object(sysfs_options, 'BONDING_DEFAULTS',
                   sysfs_options.BONDING_DEFAULTS
                   if os.path.exists(sysfs_options.BONDING_DEFAULTS)
                   else '../static/usr/share/vdsm/bonding-defaults.json')
class LinkBondSysFSTests(TestCaseBase):

    @broken_on_ci("fails on jenkins", exception=ctypes.ArgumentError)
    def test_do_not_detach_slaves_while_changing_options(self):
        OPTIONS = {'miimon': '110'}

        with dummy_devices(2) as (nic1, nic2):
            with bond_device() as bond:
                bond.add_slaves((nic1, nic2))
                mock_slaves = bond.del_slaves = bond.add_slaves = mock.Mock()

                bond.set_options(OPTIONS)

                mock_slaves.assert_not_called()


@contextmanager
def bond_device(prefix='bond_', max_length=11):
    bond_name = random_iface_name(prefix, max_length)
    bond = Bond(bond_name)
    bond.create()
    try:
        yield bond
    finally:
        bond.destroy()
