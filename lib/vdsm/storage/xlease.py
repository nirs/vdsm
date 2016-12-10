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
xlease - manage external leases
===============================

Overview
--------

External leases are stored in the xleases special volume. A lease is a
2048 blocks area at some offset in the xleases volume, associated with a
lockspace (the domain id) and a unique name. Sanlock does not manage the
mapping between the lease name and the offset of the lease; this module
removes this gap.

This module manages the mapping between Sanlock resource name and lease
offset.  When creating a lease, we find the first free slot, allocate it
for the lease, and create a sanlock resource at the associated offset.
If the xleases volume is full, we extend it to make room for more
leases. This operation must be performed only on the SPM.

Once a lease is created, any host can get the lease offset using the
lease id and use the lease offset to acquire the sanlock resource.

When removing a lease, we clear the sanlock resource and mark the slot
as free in the index. This operation must also be done on the SPM.

Sanlock keeps the lockspace name and the resource name in the lease
area.  We can rebuild the mapping from lease id to lease offset by
reading all the resources in a volume . The index is actually a cache of
the actual data on storage.


Leases volume format
--------------------

The volume format was designed so it will be possible to use the same
format in a future sanlock version that will manage the internal index
itself.

The volume is composed of "slots" where each slot is 1MiB for 512 bytes
sector size, and 8MiB for 4K sectors.

1. Lockspace slot
2. Index slot
3. Sanlock internal resource slot
4. User resources slots

The lockspace slot
------------------

In vdsm it starts at offset 0, and unused, since vdsm is using the "ids"
special volume for the lockspace. In a future storage format we may
remove the "ids" volume and use the integrated sanlock volume format.

The index slot
--------------

The index keeps the mapping between lease id and lease offset. The index
is composed of sectors, 512 bytes or 4K bytes depending on the
underlying storage.

The first block of the index is the metadata block, using this format:

- magic number (0x12152016)
- padding byte
- version (string, 4 bytes)
- padding byte
- lockspace (string, 48 bytes)
- padding
- timestamp (string, 10 bytes)
- padding
- updating flag (1 byte)
- padding
- newline

The next blocks are record blocks containing 8 records for sector size
of 512 bytes, or 64 records for sector size of 4K.

Each record contain these fields:

- resource name (string, 48 bytes)
- padding byte
- offset  (string, 11 bytes)
- padding byte
- updating flag (1 byte)
- reserved (1 byte)
- newline

The lease offset associated with a record is computed from the record
offset.  This ensures the integrity of the index; there is no way to
have two records pointing to the same offset.

To make debugging easier, the offset is also included in record itself,
but the program managing the index should never use this value.

The sanlock internal resource slot
----------------------------------

This slot is reserved for sanlock for synchronizing access to the index.
This area is not used in vdsm.

The user resources slots
------------------------

This is where user leases are created.

"""

from __future__ import absolute_import

import io
import logging
import mmap
import os
import struct

from collections import namedtuple

import six

try:
    import sanlock
except ImportError:
    if six.PY2:
        raise
    # Sanlock is not available yet in python 3, but we can still test this code
    # with fakesanlock and keep this code python 3 compatible.
    sanlock = None

from vdsm import utils
from vdsm.common.osutils import uninterruptible

# TODO: Support 4K block size.  This should be encapsulated in the Index class
# instead of being a module constant.  We can can get the block size using
# sanlock.get_alignment(), ensuring that both vdsm and sanlock are using same
# size.
from vdsm.storage.constants import BLOCK_SIZE

# Size required for Sanlock lease.
SLOT_SIZE = 2048 * BLOCK_SIZE

# Volume layout - offset from start of the volume.
LOCKSPACE_BASE = 0
INDEX_BASE = SLOT_SIZE
PRIVATE_RESOURCE_BASE = 2 * SLOT_SIZE
USER_RESOURCE_BASE = 3 * SLOT_SIZE

# The first blocks are used for index matadata
METADATA_SIZE = BLOCK_SIZE

# The offset of the first lease record from INDEX_BASE
RECORD_BASE = METADATA_SIZE

# The number of lease records supported. We can use about 16000 records, but I
# don't expect that we will need more than 2000 vm leases per data center.  To
# be on the safe size, lets double that number.  Note that we need 1GiB lease
# space for 1024 leases.
MAX_RECORDS = 4000

# Size allocated for each lease record. The minimal size is 36 bytes using uuid
# string. To simplify record number calculation, we use the next power of 2.
# We use the extra space for metadata about each lease record.
RECORD_SIZE = 64

# Each lookup will read this size from storage.
INDEX_SIZE = METADATA_SIZE + (MAX_RECORDS * RECORD_SIZE)

# lease_id \0 offset \0 updating reserved \n
RECORD_STRUCT = struct.Struct("48s x 11s x 3c")

# lease_id \0
LOOKUP_STRUCT = struct.Struct("48s x")

RECORD_TERM = b"\n"

# Sentinel for marking a free record
BLANK_LEASE = ""

# Flags
FLAG_NONE = b"-"
FLAG_UPDATING = b"u"

log = logging.getLogger("storage.xlease")

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
    msg = "Lease {self.lease_id} exists"


class LeaseUpdating(Error):
    msg = "Lease {self.lease_id} is updating"


class NoSpace(Error):
    msg = "No space to add lease {self.lease_id}"


class InvalidRecord(Error):
    msg = "Invalid record ({self.reason}): {self.record}"

    def __init__(self, reason, record):
        self.reason = reason
        self.record = record


LeaseInfo = namedtuple("LeaseInfo", (
    "lockspace",        # Sanlock lockspace name
    "resource",         # Sanlock resource name
    "path",             # Path to lease file or block device
    "offset",           # Offset in path
))


class Record(object):

    @classmethod
    def frombytes(cls, record):
        """
        Parse record data from storage and create a Record object.

        Arguments:
            record (bytes): record data, 64 bytes

        Returns:
            Record object

        Raises:
            InvalidRecord if record is not in the right format or a field
                cannot be parsed.
        """
        try:
            resource, offset, updating, _, _ = RECORD_STRUCT.unpack(record)
        except struct.error as e:
            raise InvalidRecord("cannot unpack: %s" % e, record)

        resource = resource.rstrip(b"\0")
        try:
            resource = resource.decode("ascii")
        except UnicodeDecodeError:
            raise InvalidRecord("cannot decode resource %r" % resource, record)

        updating = (updating == FLAG_UPDATING)

        try:
            offset = int(offset)
        except ValueError:
            raise InvalidRecord("cannot parse offset %r" % offset, record)

        return cls(resource, offset, updating=updating)

    def __init__(self, resource, offset, updating=False):
        """
        Initialize a record.

        Arguments:
            resource (string): UUID string
            offset (int): offset of the lease from start of volume
            updating (bool): whether record is updating
        """
        self._resource = resource
        self._offset = offset
        self._updating = updating

    def bytes(self):
        """
        Returns record data in storage format.

        Returns:
            bytes object.
        """
        return RECORD_STRUCT.pack(
            self._resource.encode("ascii"),
            b"%011d" % self._offset,
            FLAG_UPDATING if self.updating else FLAG_NONE,
            FLAG_NONE,
            RECORD_TERM,
        )

    @property
    def resource(self):
        return self._resource

    @property
    def offset(self):
        return self._offset

    @property
    def updating(self):
        return self._updating


class LeasesVolume(object):
    """
    Volume holding sanlock leases.

    The volume contains sanlock leases slots. The first lease slot is used for
    the index keeping volume metadata and the mapping from lease id to leased
    offset.

    The index is read when creating an instance, and ever read again. To read
    the data from storage, recreated the index. Changes to the instance are
    written immediately to storage.
    """

    def __init__(self, lockspace, file):
        log.debug("Loading index for lockspace %r from %r",
                  lockspace, file.name)
        self._lockspace = lockspace
        self._file = file
        self._index = VolumeIndex(file)

    @property
    def lockspace(self):
        return self._lockspace

    @property
    def path(self):
        return self._file.name

    def lookup(self, lease_id):
        """
        Lookup lease by lease_id and return LeaseInfo if found.

        Raises:
        - NoSuchLease if lease is not found.
        - InvalidRecord if corrupted lease record is found
        - OSError if io operation failed
        """
        log.debug("Looking up lease %r in lockspace %r",
                  lease_id, self._lockspace)
        recnum = self._index.find_record(lease_id)
        if recnum == -1:
            raise NoSuchLease(lease_id)

        record = self._index.read_record(recnum)
        if record.updating:
            raise LeaseUpdating(lease_id)

        offset = lease_offset(recnum)
        return LeaseInfo(self._lockspace, lease_id, self._file.name, offset)

    def add(self, lease_id):
        """
        Add lease to index, returning LeaseInfo.

        Raises:
        - LeaseExists if lease already stored for lease_id
        - InvalidRecord if corrupted lease record is found
        - NoSpace if all slots are allocated
        - OSError if I/O operation failed
        - sanlock.SanlockException if sanlock operation failed.
        """
        log.info("Adding lease %r in lockspace %r",
                 lease_id, self._lockspace)
        recnum = self._index.find_record(lease_id)
        if recnum != -1:
            record = self._index.read_record(recnum)
            if record.updating:
                # TODO: rebuild this record instead of failing
                raise LeaseUpdating(lease_id)
            else:
                raise LeaseExists(lease_id)

        recnum = self._index.find_record(BLANK_LEASE)
        if recnum == -1:
            raise NoSpace(lease_id)

        offset = lease_offset(recnum)
        record = Record(lease_id, offset, updating=True)
        self._write_record(recnum, record)

        sanlock.write_resource(self._lockspace, lease_id,
                               [(self._file.name, offset)])

        record = Record(lease_id, offset)
        self._write_record(recnum, record)

        return LeaseInfo(self._lockspace, lease_id, self._file.name, offset)

    def remove(self, lease_id):
        """
        Remove lease from index

        Raises:
        - NoSuchLease if lease was not found
        - OSError if I/O operation failed
        - sanlock.SanlockException if sanlock operation failed.
        """
        log.info("Removing lease %r in lockspace %r",
                 lease_id, self._lockspace)
        recnum = self._index.find_record(lease_id)
        if recnum == -1:
            raise NoSuchLease(lease_id)

        offset = lease_offset(recnum)
        record = Record(lease_id, offset, updating=True)
        self._write_record(recnum, record)

        # There is no way to remove a resource, so we write an invalid resource
        # with empty resource and lockspace values.
        # TODO: Use SANLK_WRITE_CLEAR, expected in rhel 7.4.
        sanlock.write_resource("", "", [(self._file.name, offset)])

        record = Record(BLANK_LEASE, offset)
        self._write_record(recnum, record)

    def leases(self):
        """
        Return all leases in the index
        """
        log.debug("Getting all leases for lockspace %r", self._lockspace)
        leases = {}
        for recnum in range(MAX_RECORDS):
            # TODO: handle bad records - currently will raise InvalidRecord and
            # fail the request.
            record = self._index.read_record(recnum)
            # Record can be:
            # - free - empty resource
            # - used - non empty resource, may be updating
            if record.resource:
                leases[record.resource] = {
                    "offset": lease_offset(recnum),
                    "updating": record.updating,
                }
        return leases

    def close(self):
        log.debug("Closing index for lockspace %r", self._lockspace)
        self._index.close()

    def _write_record(self, recnum, record):
        """
        Write record recnum to storage atomically.

        Copy the block where the record is located, modify it and write the
        block to storage. If this succeeds, write the record to the index.
        """
        block = self._index.copy_block(recnum)
        with utils.closing(block):
            block.write_record(recnum, record)
            block.dump(self._file)
        self._index.write_record(recnum, record)


def format_index(lockspace, file):
    """
    Format xleases volume index, deleting all existing records.

    Should be used only when creating a new leases volume, or if the volume
    should be repaired. Afterr formatting the index, the index can be rebuilt
    from storage contents.

    Raises:
    - OSError if I/O operation failed
    """
    log.info("Formatting index for lockspace %r", lockspace)
    # TODO:
    # - write metadata block with the updating flag
    # - dump the buffer
    # - write metadata block
    index = VolumeIndex(file)
    with utils.closing(index):
        for recnum in range(MAX_RECORDS):
            offset = lease_offset(recnum)
            record = Record(BLANK_LEASE, offset)
            index.write_record(recnum, record)
        index.dump(file)


def lease_offset(recnum):
    return USER_RESOURCE_BASE + (recnum * SLOT_SIZE)


class VolumeIndex(object):
    """
    Index maintaining volume metadata and the mapping from lease id to lease
    offset.
    """

    def __init__(self, file):
        """
        Initialize a volume index from file.
        """
        self._buf = mmap.mmap(-1, INDEX_SIZE, mmap.MAP_SHARED)
        try:
            file.seek(INDEX_BASE)
            file.readinto(self._buf)
        except:
            self._buf.close()
            raise

    def find_record(self, lease_id):
        """
        Search for lease_id record. Returns record number if found, -1
        otherwise.
        """
        prefix = LOOKUP_STRUCT.pack(lease_id.encode("ascii"))

        # TODO: continue search if offset is not aligned to record size.
        offset = self._buf.find(prefix, RECORD_BASE)
        if offset == -1:
            return -1

        return self._record_number(offset)

    def read_record(self, recnum):
        """
        Read record recnum, returns record info.
        """
        offset = self._record_offset(recnum)
        self._buf.seek(offset)
        data = self._buf.read(RECORD_SIZE)
        return Record.frombytes(data)

    def write_record(self, recnum, record):
        """
        Write record recnum to index.

        The caller is responsible for writing the record to storage before
        updating the index, otherwise the index would not reflect the state on
        storage.
        """
        offset = self._record_offset(recnum)
        self._buf.seek(offset)
        self._buf.write(record.bytes())

    def dump(self, file):
        """
        Write the entire buffer to storage and wait until the data reach
        storage. This is not atomic operation; if the operation fail, some
        blocks may not be written.
        """
        file.seek(INDEX_BASE)
        file.write(self._buf)
        os.fsync(file.fileno())

    def copy_block(self, recnum):
        offset = self._record_offset(recnum)
        block_start = offset - (offset % BLOCK_SIZE)
        return RecordBlock(self._buf, block_start)

    def close(self):
        self._buf.close()

    def _record_offset(self, recnum):
        return RECORD_BASE + recnum * RECORD_SIZE

    def _record_number(self, offset):
        return (offset - RECORD_BASE) // RECORD_SIZE


class RecordBlock(object):
    """
    A block sized buffer holding lease records.
    """

    def __init__(self, index_buf, offset):
        """
        Initialize a RecordBlock from an index buffer, copying the block
        starting at offset.

        Arguments:
            index_buf (mmap.mmap): the buffer holding the block contents
            offset (int): offset in of this block in index_buf
        """
        self._offset = offset
        self._buf = mmap.mmap(-1, BLOCK_SIZE, mmap.MAP_SHARED)
        self._buf[:] = index_buf[offset:offset + BLOCK_SIZE]

    def write_record(self, recnum, record):
        """
        Write record at recnum.

        Raises ValueError if this block does not contain recnum.
        """
        offset = self._record_offset(recnum)
        self._buf.seek(offset)
        self._buf.write(record.bytes())

    def dump(self, file):
        """
        Write the block to storage and wait until the data reach storage.

        This is atomic operation, the block is either fully written to storage
        or not.
        """
        file.seek(INDEX_BASE + self._offset)
        file.write(self._buf)
        os.fsync(file.fileno())

    def close(self):
        self._buf.close()

    def _record_offset(self, recnum):
        offset = RECORD_BASE + recnum * RECORD_SIZE - self._offset
        last_offset = BLOCK_SIZE - RECORD_SIZE
        if not 0 <= offset <= last_offset:
            raise ValueError("recnum %s out of range for this block" % recnum)
        return offset


class DirectFile(object):
    """
    File performing directio to/from mmap objects.
    """

    def __init__(self, path):
        self._path = path
        fd = os.open(path, os.O_RDWR | os.O_DIRECT)
        self._file = io.FileIO(fd, "r+", closefd=True)

    @property
    def name(self):
        return self._path

    def readinto(self, buf):
        pos = 0
        if six.PY2:
            # There is no way to create a writable memoryview on mmap object in
            # python 2, so we must read into a temporary buffer and copy into
            # the given buffer.
            rbuf = mmap.mmap(-1, len(buf), mmap.MAP_SHARED)
            with utils.closing(rbuf, log=log.name):
                while pos < len(buf):
                    # TODO: Handle EOF
                    nread = uninterruptible(self._file.readinto, rbuf)
                    buf.write(rbuf[:nread])
                    pos += nread
        else:
            # In python 3 we can read directly into the underlying buffer
            # without any copies using a memoryview.
            while pos < len(buf):
                rbuf = memoryview(buf)[pos:]
                # TODO: Handle EOF
                nread = uninterruptible(self._file.readinto, rbuf)
                pos += nread
        return pos

    def write(self, buf):
        pos = 0
        while pos < len(buf):
            if six.PY2:
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
