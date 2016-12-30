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
types - vdsm storage types

This module include the storage types mentioned in vdsm schema.  The purpose of
these types is to validate the input and provide an easy way to pass the
arguments around.
"""

from __future__ import absolute_import

from vdsm import properties
from vdsm.storage import constants as sc


class Lease(properties.Owner):
    """
    External sanlock lease.
    """
    sd_id = properties.UUID(required=True)
    lease_id = properties.UUID(required=True)

    def __init__(self, params):
        self.sd_id = params.get("sd_id")
        self.lease_id = params.get("lease_id")


class VolumeAttributes(properties.Owner):

    generation = properties.Integer(required=False, minval=0,
                                    maxval=sc.MAX_GENERATION)
    description = properties.String(required=False)

    def __init__(self, params):
        self.generation = params.get("generation")
        self.description = params.get("description")
        # TODO use properties.Enum when it supports optional enum
        self.type = params.get("type")
        # TODO use properties.Enum when it supports optional enum
        self.legality = params.get("legality")
        self._validate()

    def _validate(self):
        if self._is_empty():
            raise ValueError("No attributes to update")
        self._validate_type()
        self._validate_legality()

    def _is_empty(self):
        return (self.description is None and
                self.generation is None and
                self.legality is None and
                self.type is None)

    def _validate_type(self):
        if self.type is not None:
            if self.type != sc.type2name(sc.SHARED_VOL):
                raise ValueError("Volume type not supported %s"
                                 % self.type)

    def _validate_legality(self):
        if self.legality is not None:
            if self.legality not in [sc.LEGAL_VOL, sc.ILLEGAL_VOL]:
                raise ValueError("Legality not supported %s" % self.legality)

    def __repr__(self):
        values = ["%s=%r" % (key, value)
                  for key, value in vars(self).items()
                  if value is not None]
        return "<VolumeAttributes %s at 0x%x>" % (", ".join(values), id(self))
