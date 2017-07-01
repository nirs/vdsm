#
# Copyright 2009-2017 Red Hat, Inc.
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

CLUSTER_SIZE = 64 * 1024
SIZEOF_INT_64 = 8

# Width of refcount block entry configured in the QCOW header
REFCOUNT_ORDER = 4


def _align_offset(offset, n):
    offset = (offset + n - 1) & ~(n - 1)
    return offset


def _div_round_up(n, d):
    return (n + d - 1) // d


def _refcount_metadata_size(clusters, cluster_size, refcount_order):
    """
    Calculate the size of the refcount blocks.

    Every host cluster is reference-counted, including metadata (even
    refcount metadata is recursively included).

    An accurate formula for the size of refcount metadata size is difficult
    to derive.  An easier method of calculation is finding the fixed point
    where no further refcount blocks or table clusters are required to
    reference count every cluster.

    This is a porting of qemu source to Python.

    Arguments:
        clusters (int): number of clusters to refcount (including data and
            L1/L2 tables)
        cluster_size (int): size of a cluster, in bytes
        refcount_order (int): refcount bits power-of-2 exponent

    Returns:
        Number of bytes required for refcount blocks and table metadata.
    """
    blocks_per_table_cluster = cluster_size // SIZEOF_INT_64
    refcounts_per_block = cluster_size * 8 // (1 << refcount_order)
    table = 0   # number of refcount table clusters
    blocks = 0  # number of refcount block clusters
    last = None
    n = 0

    while n != last:
        last = n
        blocks = _div_round_up(clusters + table + blocks, refcounts_per_block)
        table = _div_round_up(blocks, blocks_per_table_cluster)
        n = clusters + blocks + table

    return (blocks + table) * cluster_size


def _estimate_metadata_size(virtual_size):
    """
    This code is ported from the qemu calculation implemented in block/qcow2.c
    in the method qcow2_create2
    """
    # int64_t aligned_total_size = align_offset(total_size, cluster_size);
    aligned_total_size = _align_offset(virtual_size, CLUSTER_SIZE)

    # Header: 1 cluster
    # meta_size += cluster_size;
    meta_size = CLUSTER_SIZE

    # Total size of L2 tables:
    #   nl2e = aligned_total_size / cluster_size;
    #   nl2e = align_offset(nl2e, cluster_size / sizeof(uint64_t));
    #   meta_size += nl2e * sizeof(uint64_t);
    nl2e = aligned_total_size // CLUSTER_SIZE
    nl2e = _align_offset(int(nl2e), CLUSTER_SIZE // SIZEOF_INT_64)
    meta_size += nl2e * SIZEOF_INT_64

    # Total size of L1 tables:
    #   nl1e = nl2e * sizeof(uint64_t) / cluster_size;
    #   nl1e = align_offset(nl1e, cluster_size / sizeof(uint64_t));
    #   meta_size += nl1e * sizeof(uint64_t);
    nl1e = nl2e * SIZEOF_INT_64 / CLUSTER_SIZE
    nl1e = _align_offset(int(nl1e), CLUSTER_SIZE // SIZEOF_INT_64)
    meta_size += nl1e * SIZEOF_INT_64

    # total size of refcount table and blocks
    meta_size += _refcount_metadata_size(
        (meta_size + aligned_total_size) // CLUSTER_SIZE,
        CLUSTER_SIZE,
        REFCOUNT_ORDER)

    return meta_size


def estimate_size(filename):
    """
    Estimating qcow2 file size once converted from raw to qcow2.
    The filename is a path (sparse or preallocated),
    or a path to preallocated block device.
    """
    info = qemuimg.info(filename)
    if (info['format'] != qemuimg.FORMAT.RAW):
        raise ValueError("Estimate size is only supported for raw format. file"
                         " %s is with format %s" % (filename, info['format']))

    # Get used clusters and virtual size of destination volume.
    virtual_size = info['virtualsize']
    meta_size = _estimate_metadata_size(virtual_size)
    runs = qemuimg.map(filename)
    used_clusters = count_clusters(runs)

    # Return the estimated size.
    return meta_size + used_clusters * CLUSTER_SIZE


def count_clusters(runs):
    count = 0
    last = -1
    for r in runs:
        # Find the cluster when start and end are located.
        start = r["start"] // CLUSTER_SIZE
        end = (r["start"] + r["length"]) // CLUSTER_SIZE
        if r["data"]:
            if start == end:
                # This run is smaller than a cluster. If we have several runs
                # in the same cluster, we want to count the cluster only once.
                if start != last:
                    count += 1
            else:
                # This run span over multiple clusters - we want to count all
                # the clusters this run touches.
                count += end - start
            last = end
    return count
