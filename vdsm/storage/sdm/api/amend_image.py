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

    def __init__(self, job_id, host_id, img_info, vol_attr):
        super(Job, self).__init__(job_id, 'amend_data', host_id)
        self._compat = vol_attr.compat
        self._img_info = img_info
        self._operation = None

    @property
    def progress(self):
        return getattr(self._operation, 'progress', None)

    def _abort(self):
        if self._operation:
            self._operation.abort()

    def _run(self):
        domId = self._img_info.domainID
        vars.task.getSharedLock(sc.STORAGE, domId)
        sd = sdCache.produce(domId)
        imgId = self._img_info.imageID
        if (self._img_info.volumeID is None):
            allVols = sd.getAllVolumes()
            imgVols = sd.getVolsOfImage(allVols, imgId)
            for vol in imgVols:
                vol = sd.produceVolume(imgId, vol.volUUID)
                self._amend_volume(vol, domId)
        else:
            vol = sd.produceVolume(imgId, vol.volUUID)
            self._amend_volume(vol, domId)
        self._operation.wait_for_completion()

    def _amend_volume(self, vol, domId):
        image_res_ns = sd.getNamespace(sc.IMAGE_NAMESPACE, domId)
        with rm.acquireResource(image_res_ns, vol.img_id, rm.EXCLUSIVE):
            vol.prepare()
            try:
                qemuimg.amend(vol, self.compat)
            finally:
                vol.teardown(domId, vol.volUUID)
