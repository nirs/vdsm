#
# Copyright 2010-2016 Red Hat, Inc.
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

from vdsm import qemuimg


# ResourceManager Lock Namespaces
STORAGE = "00_storage"
IMAGE_NAMESPACE = '01_img'
VOLUME_NAMESPACE = '02_vol'
LVM_ACTIVATION_NAMESPACE = '03_lvm'
VOLUME_LEASE_NAMESPACE = "04_lease"

SECTOR_SIZE = 512
VG_EXTENT_SIZE_MB = 128

# At the moment this is static and it has been introduced to group all the
# previous implicit references to the block size in FileVolume. In the future
# it will depend on the storage domain.
BLOCK_SIZE = 512
METADATA_SIZE = BLOCK_SIZE
VOLUME_MDNUMBLKS = 1  # The number of blocks needed to store block vol md

FILE_VOLUME_PERMISSIONS = 0o660
LEASE_FILEEXT = ".lease"

# Temporary volume indicators
TEMP_VOL_FILEEXT = ".volatile"         # Added to FileVolume metadata filenames
TEMP_VOL_LVTAG = "OVIRT_VOL_VOLATILE"  # Tag applied to BlockVolume LVs

# StorageDomain Metadata keys
MDK_POOLS = "POOL_UUID"

# Volume Types
UNKNOWN_VOL = 0
PREALLOCATED_VOL = 1
SPARSE_VOL = 2

# Volume Format
UNKNOWN_FORMAT = 3
COW_FORMAT = 4
RAW_FORMAT = 5

# Volume Role
SHARED_VOL = 6
INTERNAL_VOL = 7
LEAF_VOL = 8

VOL_TYPE = [PREALLOCATED_VOL, SPARSE_VOL]
VOL_FORMAT = [COW_FORMAT, RAW_FORMAT]
VOL_ROLE = [SHARED_VOL, INTERNAL_VOL, LEAF_VOL]

VOLUME_TYPES = {UNKNOWN_VOL: 'UNKNOWN', PREALLOCATED_VOL: 'PREALLOCATED',
                SPARSE_VOL: 'SPARSE',
                UNKNOWN_FORMAT: 'UNKNOWN', COW_FORMAT: 'COW',
                RAW_FORMAT: 'RAW',
                SHARED_VOL: 'SHARED', INTERNAL_VOL: 'INTERNAL',
                LEAF_VOL: 'LEAF'}

ILLEGAL_VOL = "ILLEGAL"
LEGAL_VOL = "LEGAL"
FAKE_VOL = "FAKE"

FMT2STR = {
    COW_FORMAT: qemuimg.FORMAT.QCOW2,
    RAW_FORMAT: qemuimg.FORMAT.RAW,
}

BLANK_UUID = "00000000-0000-0000-0000-000000000000"


def fmt2str(format):
    return FMT2STR[format]


def type2name(volType):
    try:
        return VOLUME_TYPES[volType]
    except IndexError:
        return None


def name2type(name):
    for (k, v) in VOLUME_TYPES.iteritems():
        if v == name.upper():
            return k
    return None


# Volume meta data fields
SIZE = "SIZE"
TYPE = "TYPE"
FORMAT = "FORMAT"
DISKTYPE = "DISKTYPE"
VOLTYPE = "VOLTYPE"
PUUID = "PUUID"
DOMAIN = "DOMAIN"
CTIME = "CTIME"
IMAGE = "IMAGE"
DESCRIPTION = "DESCRIPTION"
LEGALITY = "LEGALITY"
MTIME = "MTIME"
GENERATION = "GEN"  # Added in 4.1
POOL = MDK_POOLS  # Deprecated

# In block storage, metadata size is limited to BLOCK_SIZE (512), to
# ensure that metadata is written atomically. This is big enough for the
# actual metadata, but may not be big enough for the description field.
# Since a disk may be created on file storage, and moved to block
# storage, the metadata size must be limited on all types of storage.
#
# The desription field is limited to 500 characters in the engine side.
# Since ovirt 3.5, the description field is using JSON format, keeping
# both alias and description. In OVF_STORE disks, the description field
# holds additional data such as content size and date.
#
# Here is the worst case metadata format:
#
# CTIME=1440935038                            # int(time.time())
# DESCRIPTION=                                # text|JSON
# DISKTYPE=2                                  # enum
# DOMAIN=75f8a1bb-4504-4314-91ca-d9365a30692b # uuid
# FORMAT=COW                                  # RAW|COW
# IMAGE=75f8a1bb-4504-4314-91ca-d9365a30692b  # uuid
# LEGALITY=ILLEGAL                            # ILLEGAL|LEGAL|FAKE
# MTIME=0                                     # always 0
# POOL_UUID=                                  # always empty
# PUUID=75f8a1bb-4504-4314-91ca-d9365a30692b  # uuid
# SIZE=2147483648                             # size in blocks
# TYPE=PREALLOCATED                           # PREALLOCATED|UNKNOWN|SPARSE
# VOLTYPE=INTERNAL                            # INTERNAL|SHARED|LEAF
# GEN=999                                     # int
# EOF
#
# This content requires 281 bytes, leaving 231 bytes for the description
# field. OVF_STORE JSON format needs up to 175 bytes.
#
# We use a limit of 210 bytes for the description field, leaving couple
# of bytes for unexpected future changes. This should good enough for
# ascii values, but limit non-ascii values, which are encoded by engine
# using 4 bytes per character.
DESCRIPTION_SIZE = 210

# The GEN metadata key may not exist in volume metadata since it has been added
# after many volumes had been created on storage.  When missing, we default its
# value to 0 which will be written back to the metadata during the next change.
# Generation is a monotonically increasing integer that will wrap back to 0
# after reaching its maximum value.
DEFAULT_GENERATION = 0
MAX_GENERATION = 999  # Since this is represented in ASCII, limit to 3 places

# Block volume metadata tags
TAG_PREFIX_MD = "MD_"
TAG_PREFIX_MDNUMBLKS = "MS_"
TAG_PREFIX_IMAGE = "IU_"
TAG_PREFIX_PARENT = "PU_"
TAG_VOL_UNINIT = "OVIRT_VOL_INITIALIZING"
VOLUME_TAGS = [TAG_PREFIX_PARENT,
               TAG_PREFIX_IMAGE,
               TAG_PREFIX_MD,
               TAG_PREFIX_MDNUMBLKS]
