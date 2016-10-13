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
vmlease - manage vm leases volume

VM leases are taken in the vmleases special volume. For each VM we allocate
1MiB lease space at specific offset. When starting a vm, libvirt takes the
lease allocated for the vm.

The mapping between a VM id and the lease offset is maintained in the index
stored in the first MiB of the vm leases special volume.

The index format is:

block       used for
---------------------------------
0-3         metadata
4-503       lease records 0-3999
504-2047    unused

The lease offset is:

    lease_base + record_number * lease_size

Lease records are added in the first free record found.

"""

from __future__ import absolute_import

import io
import logging
import mmap
import os
import sys
import time

from collections import namedtuple

from vdsm.common.osutils import uninterruptible

# TODO: We should support 4K block size. We can can get the block size using
# sanlock.get_alignment(), ensuring that both vdsm and sanlock are using same
# alignment.
BLOCK_SIZE = 512

# Size required for Sanlock lease.
LEASE_SIZE = 2048 * BLOCK_SIZE

# The first lease slot is used for the index.
LEASE_BASE = LEASE_SIZE

# The first blocks are used for index matadata
METADATA_SIZE = 4 * BLOCK_SIZE

# The offset of the first lease record
RECORD_BASE = METADATA_SIZE

# The number of lease records supported. We can use up 16352 records, but I
# don't expect that we will need more than 2000 vm leases per storage domain.
# Note that we need 1GiB lease space for 1000 leases.
MAX_RECORDS = 4000

# Size allocated for each lease record. The minimal size is 36 bytes using uuid
# string. To simplify record number calculation, we use the next power of 2.
# We use the extra space for metadata about each lease record.
RECORD_SIZE = 64

# Each lookup will read this size from storage.
INDEX_SIZE = METADATA_SIZE + (MAX_RECORDS * RECORD_SIZE)

# Record format - everything is text to make it easy to debug using standard
# tools like less and grep.
RECORD_USED = b"USED"
RECORD_FREE = b"FREE"
SEPARATOR = b":"
TERMINATOR = b"\n"

PY2 = sys.version_info[0] == 2

# TODO: Move errors to storage.exception?


class Error(Exception):
    msg = None

    def __init__(self, lease_id):
        self.lease_id = lease_id

    def __str__(self):
        return self.msg.format(self=self)


class NoSuchLease(Error):
    msg = "No such lease {self.lease_id}"


class LeaseExists(Error):
    msg = "Lease {self.lease_id} exists since {self.modified}"

    def __init__(self, lease_id, modified):
        self.lease_id = lease_id
        self.modified = modified


class NoSpace(Error):
    msg = "No space to add lease {self.lease_id}"


class InvalidRecord(Error):
    msg = None

    def __init__(self, record):
        self.record = record


class InvalidState(InvalidRecord):
    msg = "Record with invalid state (record={self.record})"


class InvalidResource(InvalidRecord):
    msg = "Record with invalid resource name (record={self.record})"


class InvalidTimestamp(InvalidRecord):
    msg = "Record with invalid timestamp (record={self.record})"


LeaseInfo = namedtuple("LeaseInfo", (
    "lockspace",        # Sanlock lockspace name
    "resource",         # Sanlock resource name
    "path",             # Path to lease file or block device
    "offset",           # Offset in lease file
    "modified",         # Modification time in seconds since epoch
))


Record = namedtuple("Record", (
    "number",           # Record number
    "state",            # Record state (used, free, stale)
    "resource",         # Sanlock resource name
    "modified",         # Modification time in seconds since epoch
))


class Index(object):
    """
    Lease index stored at the start of the vmleases volume.

    The index is read from stroage when creating an instance, but never read
    again from storage. To update the index from storage, recreate it.

    Changes to the index are written immediately back to storage.
    """

    def __init__(self, lockspace, path):
        self._lockspace = lockspace
        self._path = path
        self._buf = IndexBuffer(DirectFile(self._path))

    @property
    def lockspace(self):
        return self._lockspace

    @property
    def path(self):
        return self._path

    def lookup(self, lease_id):
        """
        Lookup lease by lease_id and return LeaseInfo if found.

        Raises:
        - NoSuchLease if lease is not found.
        - InvalidRecord if corrupted lease record is found
        - OSError if io operation failed
        """
        # TODO: validate lease id is lower case uuid
        recnum = self._buf.find_record(lease_id)
        if recnum == -1:
            raise NoSuchLease(lease_id)

        record = self._buf.read_record(recnum)
        offset = self._lease_offset(recnum)
        return LeaseInfo(self._lockspace, lease_id, self._path, offset,
                         record.modified)

    def add(self, lease_id):
        """
        Add lease to index, returning LeaseInfo.

        This operation is atomic, modifying single block on storage.

        Raises:
        - LeaseExists if lease already stored for lease_id
        - InvalidRecord if corrupted lease record is found
        - NoSpace if all slots are allocated
        - OSError if io operation failed
        """
        # TODO: validate lease id is lower case uuid
        recnum = self._buf.find_record(lease_id)
        if recnum != -1:
            record = self._buf.read_record(recnum)
            raise LeaseExists(lease_id, record.modified)

        recnum = self._buf.find_free_record()
        if recnum == -1:
            raise NoSpace(lease_id)

        modified = int(time.time())
        self._buf.write_record(recnum, RECORD_USED, lease_id, modified)
        self._buf.flush_record(recnum)

        offset = self._lease_offset(recnum)
        return LeaseInfo(self._lockspace, lease_id, self._path, offset,
                         modified)

    def remove(self, lease_id):
        """
        Remove lease from index

        This operation is atomic, modifying single block on storage.

        Raises:
        - NoSuchLease if lease was not found
        - OSError if io operation failed
        """
        # TODO: validate lease id is lower case uuid
        recnum = self._buf.find_record(lease_id)
        if recnum == -1:
            raise NoSuchLease(lease_id)

        self._buf.clear_record(recnum)
        self._buf.flush_record(recnum)

    def format(self):
        """
        Format index, deleting all existing records.

        Raises:
        - OSError if io operation failed
        """
        # TODO: write metadata
        for recnum in range(MAX_RECORDS):
            self._buf.clear_record(recnum)
        self._buf.flush()

    def leases(self):
        """
        Return all leases in the index
        """
        leases = {}
        for recnum in range(MAX_RECORDS):
            record = self._buf.read_record(recnum)
            if record.state == RECORD_USED:
                leases[record.resource] = self._lease_offset(recnum)
        return leases

    def close(self):
        self._buf.close()

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        try:
            self.close()
        except Exception:
            if t is None:
                raise
            logging.exception("Error closing index")

    def _lease_offset(self, recnum):
        return LEASE_BASE + (recnum * LEASE_SIZE)


class IndexBuffer(object):

    def __init__(self, file):
        """
        Initialzie a buffer using file. File is owned by the buffer and will be
        closed when closing the buffer.
        """
        self._file = file
        self._buf = mmap.mmap(-1, INDEX_SIZE, mmap.MAP_SHARED)
        try:
            self._file.readinto(self._buf)
        except:
            self.close()
            raise

    def find_record(self, lease_id):
        """
        Search for lease_id record. Returns record number if found, -1
        otherwise.
        """
        prefix = (RECORD_USED +
                  SEPARATOR +
                  lease_id.encode("ascii") +
                  SEPARATOR)
        offset = self._buf.find(prefix, RECORD_BASE)
        # TODO: check alignment
        return self._record_number(offset)

    def find_free_record(self):
        """
        Search for free record, returns record number if found, -1 otherwise.
        """
        prefix = RECORD_FREE + SEPARATOR + SEPARATOR
        offset = self._buf.find(prefix, RECORD_BASE)
        # TODO: check alignment
        return self._record_number(offset)

    def read_record(self, recnum):
        """
        Read record recnum, returns record info.
        """
        offset = self._record_offset(recnum)
        self._buf.seek(offset)
        record = self._buf.read(RECORD_SIZE - 1)
        # TODO: handle bad format
        state, resource, modified, reserved = record.split(SEPARATOR, 4)
        if state not in (RECORD_USED, RECORD_FREE):
            raise InvalidState(record)
        try:
            resource = resource.decode("ascii")
        except UnicodeDecodeError:
            raise InvalidResource(record)
        try:
            modified = int(modified)
        except ValueError:
            raise InvalidTimestamp(record)

        return Record(recnum, state, resource, modified)

    def write_record(self, recnum, state, lease_id, modified):
        """
        Write record recnum with lease_id and modified time. The record is not
        written to storage; call flush_record to write it.
        """
        offset = self._record_offset(recnum)
        self._buf.seek(offset)
        self._buf.write(state)
        self._buf.write(SEPARATOR)
        self._buf.write(lease_id.encode('ascii'))
        self._buf.write(SEPARATOR)
        self._buf.write(b"%010d" % modified)
        self._buf.write(SEPARATOR)
        reserved_len = offset + RECORD_SIZE - self._buf.tell() - 1
        self._buf.write(b"0" * reserved_len)
        self._buf.write(TERMINATOR)

    def clear_record(self, recnum):
        """
        Clear record recnum. The record is not written to storage; call
        flush_record to write it.
        """
        self.write_record(recnum, RECORD_FREE, "", int(time.time()))

    def flush_record(self, recnum):
        """
        Write the block where record is located to storage and wait until the
        data reach storage.
        """
        offset = self._record_offset(recnum)
        block_start = offset - (offset % BLOCK_SIZE)
        self._file.seek(block_start)
        if PY2:
            block = buffer(self._buf, block_start, BLOCK_SIZE)
        else:
            block = memoryview(self._buf)[block_start:block_start + BLOCK_SIZE]
        self._file.write(block)
        os.fsync(self._file.fileno())

    def flush(self):
        """
        Write the entire buffer to storage and wait until the data reach
        storage. This is not atomic operation; if the operation fail, some
        blocks may not be written.
        """
        self._file.seek(0)
        self._file.write(self._buf)
        os.fsync(self._file.fileno())

    def close(self):
        self._buf.close()
        self._file.close()

    def _record_offset(self, recnum):
        return RECORD_BASE + recnum * RECORD_SIZE

    def _record_number(self, offset):
        if offset == -1:
            return -1
        return (offset - RECORD_BASE) // RECORD_SIZE


class DirectFile(object):
    """
    File performing directio to/from mmap objects.
    """

    def __init__(self, path):
        fd = os.open(path, os.O_RDWR | os.O_DIRECT)
        self._file = io.FileIO(fd, "r+", closefd=True)

    def readinto(self, buf):
        pos = 0
        if PY2:
            # There is no way to create a writable memoryview on mmap object in
            # python 2, so we must read into a temporary buffer and copy into
            # the given buffer.
            rbuf = mmap.mmap(-1, len(buf), mmap.MAP_SHARED)
            try:
                while pos < len(buf):
                    nread = uninterruptible(self._file.readinto, rbuf)
                    buf.write(rbuf[:nread])
                    pos += nread
            finally:
                rbuf.close()
        else:
            # In python 3 we can read directly into the underlying buffer
            # without any copies using a memoryview.
            while pos < len(buf):
                rbuf = memoryview(buf)[pos:]
                pos += uninterruptible(self._file.readinto, rbuf)
        return pos

    def write(self, buf):
        pos = 0
        while pos < len(buf):
            if PY2:
                wbuf = buffer(buf, pos)
            else:
                wbuf = memoryview(buf)[pos:]
            pos += uninterruptible(self._file.write, wbuf)

    def fileno(self):
        return self._file.fileno()

    def seek(self, offset, whence=os.SEEK_SET):
        return self._file.seek(offset, whence)

    def close(self):
        self._file.close()
