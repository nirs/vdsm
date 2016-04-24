#
# Copyright 2012 Red Hat, Inc.
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

from contextlib import contextmanager
import errno
from tempfile import mkstemp
import os

from vdsm import udevadm
from vdsm.utils import stopwatch
from vdsm.storage import mount

from nose.plugins.skip import SkipTest

from testlib import VdsmTestCase as TestCaseBase
from testlib import namedTemporaryDir, temporaryPath
from testlib import expandPermutations, permutations
from storage.misc import execCmd
from testValidation import ValidateRunningAsRoot
import monkeypatch

FLOPPY_SIZE = (2 ** 20) * 4


@contextmanager
def createFloppyImage(size):
    fd, path = mkstemp()
    with os.fdopen(fd, "w") as f:
        f.seek(size)
        f.write('\0')

    try:
        rc, out, err = execCmd(['/sbin/mkfs.ext2', "-F", path])
    except OSError:
        try:
            rc, out, err = execCmd(['/usr/sbin/mkfs.ext2', "-F", path])
        except OSError as e:
            if e.errno == errno.ENOENT:
                raise SkipTest("cannot execute mkfs.ext2")
            raise

    if rc != 0:
        raise Exception("Could not format image", out, err)
    try:
        yield path
    finally:
        os.unlink(path)


@expandPermutations
class TestMountEquality(TestCaseBase):

    def test_eq_equal(self):
        m1 = mount.Mount("spec", "file")
        m2 = mount.Mount("spec", "file")
        self.assertTrue(m1 == m2, "%s should equal %s" % (m1, m2))

    def test_eq_subclass(self):
        class Subclass(mount.Mount):
            pass
        m1 = mount.Mount("spec", "file")
        m2 = Subclass("spec", "file")
        self.assertFalse(m1 == m2, "%s should not equal %s" % (m1, m2))

    @permutations([
        ("spec", "spec", "file1", "file2"),
        ("spec1", "spec2", "file", "file"),
    ])
    def test_eq_different(self, spec1, spec2, file1, file2):
        m1 = mount.Mount(spec1, file1)
        m2 = mount.Mount(spec2, file2)
        self.assertFalse(m1 == m2, "%s should not equal %s" % (m1, m2))

    def test_ne_equal(self):
        m1 = mount.Mount("spec", "file")
        m2 = mount.Mount("spec", "file")
        self.assertFalse(m1 != m2, "%s should equal %s" % (m1, m2))


@expandPermutations
class TestMountHash(TestCaseBase):

    def test_equal_same_hash(self):
        m1 = mount.Mount("spec", "file")
        m2 = mount.Mount("spec", "file")
        self.assertEqual(hash(m1), hash(m2))

    def test_subclass_different_hash(self):
        class Subclass(mount.Mount):
            pass
        m1 = mount.Mount("spec", "file")
        m2 = Subclass("spec", "file")
        self.assertNotEqual(hash(m1), hash(m2))

    @permutations([
        ("spec", "spec", "file1", "file2"),
        ("spec1", "spec2", "file", "file"),
    ])
    def test_not_equal_different_hash(self, spec1, spec2, file1, file2):
        m1 = mount.Mount(spec1, file1)
        m2 = mount.Mount(spec2, file2)
        self.assertNotEqual(hash(m1), hash(m2))


class MountTests(TestCaseBase):

    @ValidateRunningAsRoot
    def testLoopMount(self):
        with namedTemporaryDir() as mpath:
            # two nested with blocks to be python 2.6 friendly
            with createFloppyImage(FLOPPY_SIZE) as path:
                m = mount.Mount(path, mpath)
                m.mount(mntOpts="loop")
                try:
                    self.assertTrue(m.isMounted())
                finally:
                    m.umount(force=True, freeloop=True)
                    # TODO: Use libudev to wait for specific event
                    with stopwatch("Wait for udev events"):
                        udevadm.settle(5)

    @ValidateRunningAsRoot
    def testSymlinkMount(self):
        with namedTemporaryDir() as root_dir:
            backing_image = os.path.join(root_dir, 'backing.img')
            link_to_image = os.path.join(root_dir, 'link_to_image')
            mountpoint = os.path.join(root_dir, 'mountpoint')
            with open(backing_image, 'w') as f:
                os.ftruncate(f.fileno(), 1024 ** 3)
            rc, out, err = execCmd(['/sbin/mkfs.ext2', "-F", backing_image],
                                   raw=True)
            if rc != 0:
                raise RuntimeError("Error creating filesystem: %s" % err)
            os.symlink(backing_image, link_to_image)
            os.mkdir(mountpoint)
            m = mount.Mount(link_to_image, mountpoint)
            m.mount(mntOpts="loop")
            try:
                self.assertTrue(m.isMounted())
            finally:
                m.umount(force=True, freeloop=True)
                # TODO: Use libudev to wait for specific event
                with stopwatch("Wait for udev events"):
                    udevadm.settle(5)


@contextmanager
def fake_mounts(mount_lines):
    """
    This method gets a list of mount lines,
    fakes the /proc/mounts and /etc/mtab files
    using monkey patch with a temporary file,
    and cleans everything on the end of use.

    Usage example:
    with fake_mounts([mount_line_1, mount_line_2]):
        <do something with /proc/mounts or /etc/mtab>
    """
    data = b"".join(line + b"\n" for line in mount_lines)
    with temporaryPath(data=data) as fake_mounts:
        with monkeypatch.MonkeyPatchScope([
            (mount, '_PROC_MOUNTS_PATH', fake_mounts),
        ]):
            yield


class TestRemoteSdIsMounted(TestCaseBase):

    def test_is_mounted(self):
        with fake_mounts([b"server:/path "
                          b"/rhev/data-center/mnt/server:_path "
                          b"nfs4 defaults 0 0"]):
            self.assertTrue(mount.isMounted(
                            b"/rhev/data-center/mnt/server:_path"))

    def test_is_mounted_deleted(self):
        with fake_mounts([b"server:/path "
                          b"/rhev/data-center/mnt/server:_path (deleted)"
                          b"nfs4 defaults 0 0"]):
            self.assertTrue(mount.isMounted(
                            b"/rhev/data-center/mnt/server:_path"))

    def test_is_not_mounted(self):
        with fake_mounts([b"server:/path "
                          b"/rhev/data-center/mnt/server:_path "
                          b"nfs4 defaults 0 0"]):
            self.assertFalse(mount.isMounted(
                             b"/rhev/data-center/mnt/server:_other_path"))
