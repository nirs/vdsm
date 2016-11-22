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
import logging

from vdsm import qemuimg
from vdsm.storage import constants as sc

from storage import resourceManager as rm
from storage import sd
from storage.sdc import sdCache

from . import base


class Job(base.Job):
    """
    Amend an image using qemu-img amend.
    """
    log = logging.getLogger('storage.sdm.amend_data')

    def __init__(self, job_id, host_id, img_info, vol_attr, repo_path):
        super(Job, self).__init__(job_id, 'amend_data', host_id)
        self._img_info = img_info
        self._vol_attr = vol_attr
        self._repo_path = repo_path

    def _run(self):
        with guarded.context(self._img_info.locks):
            with self._img_info.prepare():
                sd = sdCache.produce_manifest(self._img_info.sd_id)
                image_repo = Image(self._repo_path)
                chain = image_repo.getChain(...)

                # Remove raw base
                # XXX check if base is chain[0] or chain[-1]
                if chain[0].getFormat() != COW:
                    chain = chain[1:]

                for vol_id in chain:
                    vol = sd.produceVolume(self._img_info.img_id, vol_id)
                    qemuimg.amend(vol.getVolumePath(), self._vol_attr.compat)
