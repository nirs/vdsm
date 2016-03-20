#
# Copyright 2009-2011 Red Hat, Inc.
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
NFS helper module
"""
import os
import types
import pwd
import grp
import stat
import subprocess
import shutil
import logging
import errno
import sys

import six

from vdsm import constants

log = logging.getLogger('Storage.fileUtils')


class TarCopyFailed(RuntimeError):
    pass


def tarCopy(src, dst, exclude=()):
    excludeArgs = ["--exclude=%s" % path for path in exclude]

    tsrc = subprocess.Popen([constants.EXT_TAR, "cf", "-"] +
                            excludeArgs + ["-C", src, "."],
                            stdout=subprocess.PIPE)
    tdst = subprocess.Popen([constants.EXT_TAR, "xf", "-", "-C", dst],
                            stdin=tsrc.stdout, stderr=subprocess.PIPE,
                            stdout=subprocess.PIPE)
    tsrc.stdout.close()
    out, err = tdst.communicate()
    tsrc.wait()

    if tdst.returncode != 0 or tsrc.returncode != 0:
        raise TarCopyFailed(tsrc.returncode, tdst.returncode, out, err)


def transformPath(remotePath):
    """
    Transform remote path to new one for local mount
    """
    return remotePath.replace('_', '__').replace('/', '_')


def validateAccess(targetPath, perms=(os.R_OK | os.W_OK | os.X_OK)):
    """
    Validate the RWX access to a given path
    """
    if not os.access(targetPath, perms):
        log.warning("Permission denied for directory: %s with permissions: %s",
                    targetPath, perms)
        raise OSError(errno.EACCES, os.strerror(errno.EACCES))


def validateQemuReadable(targetPath):
    """
    Validate that qemu process can read file
    """
    gids = (grp.getgrnam(constants.DISKIMAGE_GROUP).gr_gid,
            grp.getgrnam(constants.METADATA_GROUP).gr_gid)
    st = os.stat(targetPath)
    if not (st.st_gid in gids and st.st_mode & stat.S_IRGRP or
            st.st_mode & stat.S_IROTH):
        raise OSError(errno.EACCES, os.strerror(errno.EACCES))


def pathExists(filename, writable=False):
    check = os.R_OK

    if writable:
        check |= os.W_OK

    # This function is workaround for a NFS issue where sometimes
    # os.exists/os.access fails due to NFS stale handle, in such
    # case we need to test os.access a second time.
    if os.access(filename, check):
        return True

    return os.access(filename, check)


def cleanupdir(dirPath, ignoreErrors=True):
    """
    Recursively remove all the files and directories in the given directory
    """
    cleanupdir_errors = []

    def logit(func, path, exc_info):
        cleanupdir_errors.append('%s: %s' % (func.__name__, exc_info[1]))

    log.debug("Removing directory: %s", dirPath)
    shutil.rmtree(dirPath, onerror=logit)
    if not ignoreErrors and cleanupdir_errors:
        raise RuntimeError("%s %s" % (dirPath, cleanupdir_errors))


def createdir(dirPath, mode=None):
    """
    Recursively create directory if doesn't exist

    If already exists check that permissions are as requested.
    """
    if mode is not None:
        mode = stat.S_IMODE(mode)
        params = (dirPath, mode)
    else:
        params = (dirPath,)

    log.debug("Creating directory: %s mode: %s", dirPath,
              mode if mode is None else oct(mode))
    try:
        os.makedirs(*params)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
        statinfo = os.stat(dirPath)
        if not stat.S_ISDIR(statinfo.st_mode):
            raise OSError(errno.ENOTDIR, "Not a directory %s" % dirPath)
        log.warning("Dir %s already exists", dirPath)
        if mode is not None:
            curMode = stat.S_IMODE(statinfo.st_mode)
            if curMode != mode:
                raise OSError(errno.EPERM,
                              ("Existing %s permissions %o are not as "
                               "requested %o") % (dirPath, curMode, mode))


def resolveUid(user):
    if isinstance(user, types.StringTypes):
        uid = pwd.getpwnam(user).pw_uid
    else:
        uid = int(user)
    return uid


def resolveGid(group):
    if isinstance(group, types.StringTypes):
        gid = grp.getgrnam(group).gr_gid
    else:
        gid = int(group)
    return gid


def chown(path, user=-1, group=-1):
    """
    Change the owner and\or group of a file.
    The user and group parameters can either be a name or an id.
    """
    uid = resolveUid(user)
    gid = resolveGid(group)

    stat = os.stat(path)
    currentUid = stat.st_uid
    currentGid = stat.st_gid

    if ((uid == currentUid or user == -1) and
            (gid == currentGid or group == -1)):
        return True
    log.debug("Changing owner for %s, to (%s:%s)", path, uid, gid)
    os.chown(path, uid, gid)
    return True


def atomic_symlink(target, name):
    """
    Create s symbolic link atomically, updating stale links.

    If the symlink exists but links to a different target, it is replaced
    atomically with a link to the requested target.

    If the process is killed while creating a link, it may leave temporary link
    (name.tmp). This link will be removed in the next time a link is created.

    This replace a link atomically, so you will have either the old link, or
    the new link. However it is not safe to call this from multiple threads,
    trying to modify the same link.
    """
    log.info("Linking %r to %r", target, name)
    try:
        current_target = os.readlink(name)
    except OSError as e:
        if e.errno != errno.ENOENT:
            raise
    else:
        if current_target == target:
            log.debug("link %r exists", name)
            return
        log.debug("Replacing stale link to %r", current_target)

    tmp_name = name + ".tmp"
    try:
        os.symlink(target, tmp_name)
    except OSError as e:
        if e.errno != errno.EEXIST:
            raise
        log.debug("Removing stale temporary link %r", tmp_name)
        os.unlink(tmp_name)
        os.symlink(target, tmp_name)

    try:
        os.rename(tmp_name, name)
    except:
        exc_info = sys.exc_info()
        try:
            os.unlink(tmp_name)
        except OSError as e:
            log.error("Cannot remove temporary link %r: %s", tmp_name, e)
        six.reraise(*exc_info)


def fsyncPath(path):
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def copyUserModeToGroup(path):
    mode = os.stat(path).st_mode
    userMode = mode & 0o700  # user mode mask
    newGroupMode = userMode >> 3
    if (mode & 0o070) != newGroupMode:  # group mode mask
        # setting the new group mode masking out the original one
        newMode = (mode & 0o707) | newGroupMode
        log.debug("Changing mode for %s to %#o", path, newMode)
        os.chmod(path, newMode)


def padToBlockSize(path):
    with open(path, 'a') as f:
        size = os.fstat(f.fileno()).st_size
        newSize = 512 * ((size + 511) / 512)
        log.debug("Truncating file %s to %d bytes", path, newSize)
        os.ftruncate(f.fileno(), newSize)
