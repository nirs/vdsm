#
# Copyright 2012-2016 Red Hat, Inc.
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
from __future__ import absolute_import


class VdsmException(Exception):
    code = 0
    message = "Vdsm Exception"

    def __str__(self):
        return self.message

    def info(self):
        return {'code': self.code, 'message': str(self)}

    def response(self):
        return {'status': self.info()}


class ContextException(VdsmException):
    """
    Adds reason and context arguments for better error messages.

    This is a temporary class to be used while we convert old exceptions to the
    new calling style. Once all calls are using kwargs call style, we merge
    this class into VdsmException.
    """

    def __init__(self, reason=None, **kwargs):
        """
        There are 3 ways to initialize an instance:

        - no arguments - discouraged in general, but there may be valid use
          cases for this.
        - reason only - prevent unexpected failures in runtime when trying to
          use this exception in the usual way.
        - reason and kwargs - the recommended way to use this class.

        All the arguments are stored in the context instance variable.
        """
        self.context = kwargs
        if reason:
            self.context["reason"] = reason

    def __str__(self):
        if self.context:
            return "%s: %s" % (self.message, self.context)
        else:
            return self.message


class NoSuchVM(ContextException):
    code = 1
    message = 'Virtual machine does not exist'


# code 2: unused


class AccessTimeout(VdsmException):
    code = 3
    message = 'Image repository access timeout'


class VMExists(VdsmException):
    code = 4
    message = 'Virtual machine already exists'


class UnsupportedVMType(VdsmException):
    code = 5
    message = 'Unsupported VM type'


class VMIsDown(VdsmException):
    code = 6
    message = 'Virtual machine is down'


class CopyFailed(VdsmException):
    code = 7
    message = 'Copy failed'


class CannotCreateSparse(VdsmException):
    code = 8
    message = 'Sparse creation faild'


class CannotCreateVM(VdsmException):
    code = 9
    message = 'Error creating the requested VM'


class NoConnectionToPeer(VdsmException):
    code = 10
    message = 'Could not connect to peer VDS'


class MissingParameter(VdsmException):
    code = 11
    message = 'Missing required parameter'


class MigrationError(VdsmException):
    code = 12
    message = 'Fatal error during migration'


class ImageFileNotFound(VdsmException):
    code = 13
    message = 'Drive image file could not be found'


class OutOfMemory(VdsmException):
    code = 14
    message = 'Not enough free memory to create VM'


class UnexpectedError(VdsmException):
    code = 16
    message = 'Unexpected exception'


class UnsupportedImageFormat(VdsmException):
    code = 17
    message = 'Unsupported image format'


class SpiceTicketError(VdsmException):
    code = 18
    message = 'Error while setting spice ticket'


class NonResponsiveGuestAgent(VdsmException):
    code = 19
    message = 'Guest agent non-responsive'


# codes 20-35 are reserved for add/delNetwork
# code 39 was used for:
# wrongHost - migration destination has an invalid hostname


class ResourceUnavailable(VdsmException):
    code = 40
    message = 'Resource unavailable'


class ChangeDiskFailed(VdsmException):
    code = 41
    message = 'Failed to change disk image'


class VMDestroyFailed(VdsmException):
    code = 42
    message = 'Virtual machine destroy error'


class UnsupportedFenceAgent(VdsmException):
    code = 43
    message = 'Unsupported fencing agent'


class MethodNotImplemented(VdsmException):
    code = 44
    message = 'Not implemented'


class HotplugDiskFailed(VdsmException):
    code = 45
    message = 'Failed to hotplug disk'


class HotunplugDiskFailed(VdsmException):
    code = 46
    message = 'Failed to hotunplug disk'


class MigrationCancelationFailed(VdsmException):
    code = 47
    message = 'Migration not in progress'


class SnapshotFailed(VdsmException):
    code = 48
    message = 'Snapshot failed'


class HotplugNicFailed(VdsmException):
    code = 49
    message = 'Failed to hotplug NIC'


class HotunplugNicFailed(VdsmException):
    code = 50
    message = 'Failed to hotunplug NIC'


class MigrationInProgress(ContextException):
    code = 51
    message = 'Command not supported during migration'


class MergeFailed(VdsmException):
    code = 52
    message = 'Merge failed'


class BalloonError(VdsmException):
    code = 53
    message = 'Balloon operation is not available'


class MOMPolicyUpdateFailed(VdsmException):
    code = 54
    message = 'Failed to set mom policy'


class ReplicaError(VdsmException):
    code = 55
    message = 'Drive replication error'


class UpdateDeviceFailed(VdsmException):
    code = 56
    message = 'Failed to update device'


class CannotRetrieveHWInfo(VdsmException):
    code = 57
    message = 'Failed to read hardware information'


class BadDiskResizeParameter(VdsmException):
    code = 58
    message = 'Wrong resize disk parameter'


class TransientError(VdsmException):
    code = 59
    message = 'Action not permitted on a VM with transient disks'


class SetNumberOfCpusFailed(VdsmException):
    code = 60
    message = 'Failed to set the number of cpus'


class SetHAPolicyFailed(VdsmException):
    code = 61
    message = 'Failed to set Hosted Engine HA policy'


class CpuTuneError(VdsmException):
    code = 62
    message = 'CpuTune operation is not available'


class UpdateVMPolicyFailed(VdsmException):
    code = 63
    message = 'Failed to update VM SLA policy'


class UpdateIOTuneError(VdsmException):
    code = 64
    message = 'Failed to update ioTune values'


class V2VConnectionError(VdsmException):
    code = 65
    message = 'error connecting to hypervisor'


class NoSuchJob(VdsmException):
    code = 66
    message = 'Job Id does not exists'


class V2VNoSuchOVF(VdsmException):
    code = 67
    message = 'OVF file does not exists'


class JobNotDone(VdsmException):
    code = 68
    message = 'Job status is not done'


class JobExists(VdsmException):
    code = 69
    message = 'Job id already exists'


class HotplugMemFailed(VdsmException):
    code = 70
    message = 'Failed to hotplug memory'


class KSMUpdateFailed(VdsmException):
    code = 71
    message = 'Failed to update KSM values'


class BadSecretRequest(VdsmException):
    code = 72
    message = 'Bad secret request'


class SecretRegistrationFailed(VdsmException):
    code = 73
    message = 'Error registering Libvirt secret'


class SecretUnregistrationFailed(VdsmException):
    code = 74
    message = 'Error unregistering Libvirt secret'


class UnsupportedOperation(VdsmException):
    code = 75
    message = 'Operation not supported'


class FreezeGuestFSFailed(VdsmException):
    code = 76
    message = 'Unable to freeze guest filesystems'


class ThawGuestFSFailed(VdsmException):
    code = 77
    message = 'Unable to thaw guest filesystems'


class HookFailed(VdsmException):
    code = 78
    message = 'Hook error'


class DestinationVolumeTooSmall(VdsmException):
    code = 79
    message = 'Destination volume is too small'


class AbortNotSupported(VdsmException):
    code = 80
    message = 'Job does not support aborting'


class MigrationNotInProgress(VdsmException):
    code = 81
    message = 'Migration not in progress'


class MigrationLimitExceeded(VdsmException):
    code = 82
    message = 'Incoming migration limit exceeded'


class HostdevDetachFailed(VdsmException):
    code = 83
    message = 'Could not detach host device'


class JobNotActive(VdsmException):
    code = 84
    message = 'Job is not active'


class HotunplugMemFailed(ContextException):
    code = 85
    message = 'Failed to hotunplug memory'


class HotplugLeaseFailed(ContextException):
    code = 86
    message = 'Failed to hotplug lease'


class HotunplugLeaseFailed(ContextException):
    code = 87
    message = 'Failed to hotunplug lease'


class RecoveryInProgress(VdsmException):
    code = 99
    message = 'Recovering from crash or Initializing'


class GeneralException(VdsmException):
    code = 100
    message = "General Exception"

    def __init__(self, *value):
        self.value = value

    def __str__(self):
        return "%s: %s" % (self.message, repr(self.value))


class InvalidConfiguration(GeneralException):
    code = 101
    message = "Invalid configuration value"


class ActionStopped(GeneralException):
    code = 443
    message = "Action was stopped"


class HookError(GeneralException):
    code = 1500
    message = "Hook Error"
