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

import logging
import signal

from vdsm import commands
from vdsm import constants
from vdsm.storage import exception as se

import curlImgWrap

log = logging.getLogger("Storage.ImageSharing")
# Time to wait from finishing writing data to dd, until dd exists,
# Ensure that we don't keep the task active forever if dd cannot
# access the storage.
WAIT_TIMEOUT = 30
# Number of bytes to read from the socket and write
# to dd stdin trough the pipe. Based on default socket buffer
# size(~80KB) and default pipe buffer size (64K), this should
# minimize system call overhead without consuming too much
# memory.
BUFFER_SIZE = 65536


def httpGetSize(methodArgs):
    headers = curlImgWrap.head(methodArgs.get('url'),
                               methodArgs.get("headers", {}))

    size = None

    if 'Content-Length' in headers:
        size = int(headers['Content-Length'])

    # OpenStack Glance returns Content-Length = 0 so we need to
    # override the value with the content of the custom header
    # X-Image-Meta-Size.
    if 'X-Image-Meta-Size' in headers:
        size = max(size, int(headers['X-Image-Meta-Size']))

    if size is None:
        raise RuntimeError("Unable to determine image size")

    return size


def getLengthFromArgs(methodArgs):
    return methodArgs['length']


def httpDownloadImage(dstImgPath, methodArgs):
    curlImgWrap.download(methodArgs.get('url'), dstImgPath,
                         methodArgs.get("headers", {}))


def httpUploadImage(srcImgPath, methodArgs):
    curlImgWrap.upload(methodArgs.get('url'), srcImgPath,
                       methodArgs.get("headers", {}))


def copyToImage(dstImgPath, methodArgs):
    totalSize = getLengthFromArgs(methodArgs)
    fileObj = methodArgs['fileObj']
    cmd = [constants.EXT_DD, "of=%s" % dstImgPath, "bs=%s" % constants.MEGAB]
    p = commands.execCmd(cmd, sync=False, deathSignal=signal.SIGKILL)
    try:
        _copyData(fileObj, p.stdin, totalSize)
        p.stdin.close()
        if not p.wait(WAIT_TIMEOUT):
            log.error("timeout waiting for dd process")
            raise se.StorageException()

        if p.returncode != 0:
            log.error("dd error - code %s, stderr %s",
                      p.returncode, p.stderr.read(1000))
            raise se.MiscFileWriteException()

    except Exception:
        if p.returncode is None:
            p.kill()
        raise


def copyFromImage(dstImgPath, methodArgs):
    fileObj = methodArgs['fileObj']
    bytes_left = total_size = methodArgs['length']
    cmd = [constants.EXT_DD, "if=%s" % dstImgPath, "bs=%s" % constants.MEGAB,
           "count=%s" % (total_size / constants.MEGAB + 1)]

    p = commands.execCmd(cmd, sync=False,
                         deathSignal=signal.SIGKILL)
    p.blocking = True
    try:
        _copyData(p.stdout, fileObj, bytes_left)
    finally:
        if p.returncode is None:
            p.kill()


def _copyData(inFile, outFile, totalSize):
    bytesToRead = totalSize
    while totalSize > 0:
        toRead = min(BUFFER_SIZE, totalSize)

        try:
            data = inFile.read(toRead)
        except IOError as e:
            error = "error reading file: %s" % e
            log.error(error)
            raise se.MiscFileReadException(error)

        if not data:
            error = "partial data %s from %s" % \
                    (bytesToRead - totalSize, bytesToRead)
            log.error(error)
            raise se.MiscFileReadException(error)

        outFile.write(data)
        # outFile may not be a real file object but a wrapper.
        # To ensure that we don't use more memory as the input buffer size
        # we flush on every write.
        outFile.flush()

        totalSize = totalSize - len(data)


_METHOD_IMPLEMENTATIONS = {
    'http': (httpGetSize, httpDownloadImage, httpUploadImage),
}


def _getSharingMethods(methodArgs):
    try:
        method = methodArgs['method']
    except KeyError:
        raise RuntimeError("Sharing method not specified")

    try:
        return _METHOD_IMPLEMENTATIONS[method]
    except KeyError:
        raise RuntimeError("Sharing method %s not found" % method)


def getSize(methodArgs):
    getSizeImpl, _, _ = _getSharingMethods(methodArgs)
    return getSizeImpl(methodArgs)


def download(dstImgPath, methodArgs):
    _, downloadImageImpl, _ = _getSharingMethods(methodArgs)
    downloadImageImpl(dstImgPath, methodArgs)


def upload(srcImgPath, methodArgs):
    _, _, uploadImageImpl = _getSharingMethods(methodArgs)
    uploadImageImpl(srcImgPath, methodArgs)
