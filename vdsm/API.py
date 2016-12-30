#
# Copyright (C) 2012 Adam Litke, IBM Corporation
# Copyright (C) 2012-2016 Red Hat, Inc.
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
# pylint: disable=R0904

import os

from vdsm.network.errors import ConfigNetworkError

from vdsm import commands
from vdsm import utils
from clientIF import clientIF
from vdsm import constants
from vdsm import logUtils
from vdsm import hooks
from vdsm import hostdev
from vdsm import supervdsm
from vdsm import throttledlog
from vdsm import jobs
from vdsm import v2v
from vdsm.common import api
from vdsm.common import exception
from vdsm.common import response
from vdsm.host import api as hostapi
from vdsm.host import caps
from vdsm.logUtils import AllVmStatsValue, Suppressed
from vdsm.storage import clusterlock
from vdsm.storage import misc
from vdsm.storage import constants as sc
from vdsm.virt import migration
from vdsm.virt import secret
import storage.volume
import storage.sd
import storage.image
from virt.vmdevices import graphics
from virt.vmdevices import hwclass
from vdsm.compat import pickle
from vdsm.common.define import doneCode, errCode
from vdsm.config import config
from vdsm.virt import sampling


haClient = None  # Define here to work around pyflakes issue #13
try:
    import ovirt_hosted_engine_ha.client.client as haClient
except ImportError:
    pass

try:
    import vdsm.gluster.fence as glusterFence
except ImportError:
    pass


# default message for system shutdown, will be displayed in guest
USER_SHUTDOWN_MESSAGE = 'System going down'


throttledlog.throttle('getAllVmStats', 100)


def updateTimestamp():
    # The setup API uses this log file to determine if this host is still
    # accessible.  We use a file (rather than an event) because setup is
    # performed by a separate, root process.
    utils.touchFile(constants.P_VDSM_CLIENT_LOG)


class APIBase(object):
    ctorArgs = []

    def __init__(self):
        self._cif = clientIF.getInstance()
        self._irs = self._cif.irs
        self.log = self._cif.log


class Task(APIBase):
    ctorArgs = ['taskID']

    def __init__(self, UUID):
        APIBase.__init__(self)
        self._UUID = UUID

    def clear(self):
        return self._irs.clearTask(self._UUID)

    def getInfo(self):
        return self._irs.getTaskInfo(self._UUID)

    def getStatus(self):
        return self._irs.getTaskStatus(self._UUID)

    def revert(self):
        return self._irs.revertTask(self._UUID)

    def stop(self):
        return self._irs.stopTask(self._UUID)


class VM(APIBase):
    BLANK_UUID = '00000000-0000-0000-0000-000000000000'
    ctorArgs = ['vmID']

    def __init__(self, UUID):
        APIBase.__init__(self)
        self._UUID = UUID

    def changeCD(self, driveSpec):
        """
        Change the CD in the specified VM.

        :param vmId: uuid of specific VM.
        :type vmId: UUID
        :param driveSpec: specification of the new CD image. Either an
                image path or a `storage`-centric quartet.
        """
        v = self._cif.vmContainer.get(self._UUID)
        if not v:
            return errCode['noVM']
        return v.changeCD(driveSpec)

    def changeFloppy(self, driveSpec):
        """
        Change the floppy disk in the specified VM.

        :param vmId: uuid of specific VM.
        :type vmId: UUID
        :param driveSpec: specification of the new CD image. Either an
                image path or a `storage`-centric quartet.
        """
        v = self._cif.vmContainer.get(self._UUID)
        if not v:
            return errCode['noVM']
        return v.changeFloppy(driveSpec)

    def cont(self):
        v = self._cif.vmContainer.get(self._UUID)
        if not v:
            return errCode['noVM']
        return v.cont()

    @api.method
    def create(self, vmParams):
        """
        Start up a virtual machine.

        :param vmParams: required and optional VM parameters.
        :type vmParams: dict
        """
        vmParams['vmId'] = self._UUID
        try:
            if vmParams.get('vmId') in self._cif.vmContainer:
                self.log.warning('vm %s already exists' % vmParams['vmId'])
                raise exception.VMExists()

            if 'hiberVolHandle' in vmParams:
                vmParams['restoreState'], paramFilespec = \
                    self._getHibernationPaths(vmParams.pop('hiberVolHandle'))
                try:   # restore saved vm parameters
                    # NOTE: pickled params override command-line params. this
                    # might cause problems if an upgrade took place since the
                    # parmas were stored.
                    fname = self._cif.prepareVolumePath(paramFilespec)
                    try:
                        with open(fname) as f:
                            pickledMachineParams = pickle.load(f)

                        if type(pickledMachineParams) == dict:
                            self.log.debug('loaded pickledMachineParams ' +
                                           str(pickledMachineParams))
                            self.log.debug('former conf ' + str(vmParams))
                            vmParams.update(pickledMachineParams)
                    finally:
                        self._cif.teardownVolumePath(paramFilespec)
                except:
                    self.log.error("Error restoring VM parameters",
                                   exc_info=True)

            self._validate_vm_params(vmParams)

            self._fix_vm_params(vmParams)

            if 'sysprepInf' in vmParams:
                if not self._createSysprepFloppyFromInf(vmParams['sysprepInf'],
                                                        vmParams['floppy']):
                    raise exception.CannotCreateVM(
                        'Failed to create sysprep floppy image. '
                        'No space on /tmp?')

            if not graphics.isSupportedDisplayType(vmParams):
                raise exception.CannotCreateVM(
                    'Unknown display type %s' % vmParams.get('display'))
            return self._cif.createVm(vmParams)

        except OSError as e:
            self.log.debug("OS Error creating VM", exc_info=True)
            raise exception.CannotCreateVM(
                'Failed to create VM. No space on /tmp? %s' % e.message)
        except exception.VdsmException:
            # TODO: remove when the transition to @api.method is completed.
            raise  # do not interfer with api.method()
        except:
            self.log.debug("Error creating VM", exc_info=True)
            raise exception.UnexpectedError()

    def _validate_vm_params(self, vmParams):
        requiredParams = ['vmId', 'memSize']
        for param in requiredParams:
            if param not in vmParams:
                self.log.error('Missing required parameter %s' % (param))
                raise exception.MissingParameter(
                    'Missing required parameter %s' % (param))
        try:
            misc.validateUUID(vmParams['vmId'])
        except:
            raise exception.MissingParameter('vmId must be a valid UUID')
        if vmParams['memSize'] == 0:
            raise exception.MissingParameter(
                'Must specify nonzero memSize')

        if vmParams.get('boot') == 'c' and 'hda' not in vmParams \
                and not vmParams.get('drives'):
            raise exception.MissingParameter('missing boot disk')

    def _fix_vm_params(self, vmParams):
        if 'vmType' not in vmParams:
            vmParams['vmType'] = 'kvm'
        elif vmParams['vmType'] == 'kvm':
            if 'kvmEnable' not in vmParams:
                vmParams['kvmEnable'] = 'true'

        if 'sysprepInf' in vmParams:
            if not vmParams.get('floppy'):
                vmParams['floppy'] = '%s%s.vfd' % (
                    constants.P_VDSM_RUN, vmParams['vmId'])
            vmParams['volatileFloppy'] = True
        return vmParams

    def desktopLock(self):
        """
        Lock user session in guest operating system using guest agent.
        """
        try:
            v = self._cif.vmContainer[self._UUID]
        except KeyError:
            return errCode['noVM']
        v.guestAgent.desktopLock()
        if v.guestAgent.isResponsive():
            return {'status': doneCode}
        else:
            return errCode['nonresp']

    def desktopLogin(self, domain, username, password):
        """
        Log into guest operating system using guest agent.
        """
        try:
            v = self._cif.vmContainer[self._UUID]
        except KeyError:
            return errCode['noVM']
        v.guestAgent.desktopLogin(domain, username, password)
        if v.guestAgent.isResponsive():
            return {'status': doneCode}
        else:
            return errCode['nonresp']

    def desktopLogoff(self, force):
        """
        Log out of guest operating system using guest agent.
        """
        try:
            v = self._cif.vmContainer[self._UUID]
        except KeyError:
            return errCode['noVM']
        v.guestAgent.desktopLogoff(force)
        if v.guestAgent.isResponsive():
            return {'status': doneCode}
        else:
            return errCode['nonresp']

    def desktopSendHcCommand(self, message):
        """
        Send a command to the guest agent (depricated).
        """
        try:
            v = self._cif.vmContainer[self._UUID]
        except KeyError:
            return errCode['noVM']
        v.guestAgent.sendHcCmdToDesktop(message)
        if v.guestAgent.isResponsive():
            return {'status': doneCode}
        else:
            return errCode['nonresp']

    def destroy(self, gracefulAttempts=1):
        """
        Destroy the specified VM.
        """
        self.log.debug('About to destroy VM %s', self._UUID)

        with self._cif.vmContainerLock:
            v = self._cif.vmContainer.get(self._UUID)
            if not v:
                return errCode['noVM']
            res = v.destroy(gracefulAttempts)
            status = utils.picklecopy(res)
            if status['status']['code'] == 0:
                status['status']['message'] = "Machine destroyed"
            return status

    def getMigrationStatus(self):
        """
        Report status of a currently outgoing migration.
        """
        # No longer called from Engine >= 4.1, replaced by events.
        try:
            v = self._cif.vmContainer[self._UUID]
        except KeyError:
            return errCode['noVM']
        return {'status': doneCode, 'migrationStats': v.migrateStatus()}

    def getStats(self):
        """
        Obtain statistics of the specified VM
        """
        v = self._cif.vmContainer.get(self._UUID)
        if not v:
            return errCode['noVM']

        try:
            hooks.before_get_vm_stats()
        except exception.HookError as e:
            return response.error('hookError',
                                  'Hook error: ' + str(e))

        stats = v.getStats().copy()
        stats = hooks.after_get_vm_stats([stats])[0]
        return {'status': doneCode, 'statsList': [stats]}

    def hibernate(self, hibernationVolHandle):
        """
        Hibernate a VM.

        :param hiberVolHandle: opaque string, indicating the location of
                               hibernation images.
        """
        params = {'vmId': self._UUID, 'mode': 'file',
                  'hiberVolHandle': hibernationVolHandle}
        response = self.migrate(params)
        if not response['status']['code']:
            response['status']['message'] = 'Hibernation process starting'
        return response

    def updateDevice(self, params):
        if 'deviceType' not in params:
            self.log.error('Missing a required parameters: deviceType')
            return {'status': {'code': errCode['MissParam']['status']['code'],
                               'message': 'Missing one of required '
                                          'parameters: deviceType'}}
        try:
            v = self._cif.vmContainer[self._UUID]
        except KeyError:
            self.log.warning("vm %s doesn't exist", self._UUID)
            return errCode['noVM']

        if params['deviceType'] == hwclass.NIC:
            if 'alias' not in params:
                self.log.error('Missing the required alias parameters.')
                return {'status':
                        {'code': errCode['MissParam']['status']['code'],
                         'message': 'Missing the required alias parameter'}}
        return v.updateDevice(params)

    def hotplugNic(self, params):
        try:
            utils.validateMinimalKeySet(params, ('vmId', 'nic'))
        except ValueError:
            self.log.error('Missing one of required parameters: vmId, nic')
            return {'status': {'code': errCode['MissParam']['status']['code'],
                               'message': 'Missing one of required '
                                          'parameters: vmId, nic'}}
        try:
            curVm = self._cif.vmContainer[self._UUID]
        except KeyError:
            self.log.warning("vm %s doesn't exist", self._UUID)
            return errCode['noVM']

        return curVm.hotplugNic(params)

    def hostdevHotplug(self, devices):
        try:
            curVm = self._cif.vmContainer[self._UUID]
        except KeyError:
            self.log.warning("vm %s doesn't exist", self._UUID)
            return errCode['noVM']

        return curVm.hostdevHotplug(devices)

    def hostdevHotunplug(self, devices):
        try:
            curVm = self._cif.vmContainer[self._UUID]
        except KeyError:
            self.log.warning("vm %s doesn't exist", self._UUID)
            return errCode['noVM']

        return curVm.hostdevHotunplug(devices)

    def hotunplugNic(self, params):
        try:
            utils.validateMinimalKeySet(params, ('vmId', 'nic'))
        except ValueError:
            self.log.error('Missing one of required parameters: vmId, nic')
            return {'status': {'code': errCode['MissParam']['status']['code'],
                               'message': 'Missing one of required '
                                          'parameters: vmId, nic'}}
        try:
            curVm = self._cif.vmContainer[self._UUID]
        except KeyError:
            self.log.warning("vm %s doesn't exist", self._UUID)
            return errCode['noVM']

        return curVm.hotunplugNic(params)

    def hotplugDisk(self, params):
        try:
            utils.validateMinimalKeySet(params, ('vmId', 'drive'))
        except ValueError:
            self.log.error('Missing one of required parameters: vmId, drive')
            return {'status': {'code': errCode['MissParam']['status']['code'],
                               'message': 'Missing one of required '
                                          'parameters: vmId, drive'}}
        try:
            curVm = self._cif.vmContainer[self._UUID]
        except KeyError:
            self.log.warning("vm %s doesn't exist", self._UUID)
            return errCode['noVM']

        return curVm.hotplugDisk(params)

    def hotunplugDisk(self, params):
        try:
            utils.validateMinimalKeySet(params, ('vmId', 'drive'))
        except ValueError:
            self.log.error('Missing one of required parameters: vmId, drive')
            return {'status': {'code': errCode['MissParam']['status']['code'],
                               'message': 'Missing one of required '
                                          'parameters: vmId, drive'}}
        try:
            curVm = self._cif.vmContainer[self._UUID]
        except KeyError:
            self.log.warning("vm %s doesn't exist", self._UUID)
            return errCode['noVM']

        return curVm.hotunplugDisk(params)

    def hotplugMemory(self, params):
        try:
            utils.validateMinimalKeySet(params, ('vmId', 'memory'))
        except ValueError:
            self.log.error('Missing one of required parameters: vmId, memory')
            return {'status': {'code': errCode['MissParam']['status']['code'],
                               'message': 'Missing one of required '
                                          'parameters: vmId, memory'}}

        try:
            curVm = self._cif.vmContainer[self._UUID]
        except KeyError:
            self.log.warning("vm %s doesn't exist", self._UUID)
            return errCode['noVM']

        return curVm.hotplugMemory(params)

    def setNumberOfCpus(self, numberOfCpus):

        if self._UUID is None or numberOfCpus is None:
            self.log.error('Missing one of required parameters: \
            vmId: (%s), numberOfCpus: (%s)', self._UUID, numberOfCpus)
            return {'status': {'code': errCode['MissParam']['status']['code'],
                               'message': 'Missing one of required '
                                          'parameters: vmId, numberOfCpus'}}
        try:
            curVm = self._cif.vmContainer[self._UUID]
        except KeyError:
            self.log.warning("vm %s doesn't exist", self._UUID)
            return errCode['noVM']

        return curVm.setNumberOfCpus(int(numberOfCpus))

    def updateVmPolicy(self, params):
        try:
            curVm = self._cif.vmContainer[self._UUID]
        except KeyError:
            self.log.warning("vm %s doesn't exist", self._UUID)
            return errCode['noVM']

        # Remove the vmId parameter from params we do not need it anymore
        del params["vmId"]

        return curVm.updateVmPolicy(params)

    def migrate(self, params):
        """
        Migrate a VM to a remote host.

        :param params: a dictionary containing:
            *dst* - remote host or hibernation image filename
            *dstparams* - hibernation image filename for vdsm parameters
            *mode* - ``remote``/``file``
            *method* - ``online``
            *downtime* - allowed down time during online migration
            *consoleAddress* - remote host graphics address
            *dstqemu* - remote host address dedicated for migration
            *compressed* - compress repeated pages during live migration
            *autoConverge* - force convergence during live migration
            *maxBandwidth* - max bandwidth used by this specific migration
            *convergenceSchedule* - actions to perform when stalling
            *outgoingLimit* - max number of outgoing migrations, must be > 0.
            *incomingLimit* - max number of incoming migrations, must be > 0.
        """
        params['vmId'] = self._UUID
        self.log.debug(params)
        try:
            v = self._cif.vmContainer[self._UUID]
        except KeyError:
            return errCode['noVM']

        if params.get('mode') == 'file':
            if 'dst' not in params:
                params['dst'], params['dstparams'] = \
                    self._getHibernationPaths(params['hiberVolHandle'])
        else:
            params['mode'] = 'remote'
        return v.migrate(params)

    def migrateChangeParams(self, params):
        """
        Change parameters of an ongoing migration

        :param params: a dictionary containing:
            *maxBandwidth* - new max bandwidth
        """
        try:
            v = self._cif.vmContainer[self._UUID]
        except KeyError:
            return response.error('noVM')
        return v.migrateChangeParams(params)

    def migrateCancel(self):
        """
        Cancel a currently outgoing migration process.
        """
        try:
            v = self._cif.vmContainer[self._UUID]
        except KeyError:
            return errCode['noVM']
        return v.migrateCancel()

    def migrationCreate(self, params, incomingLimit=None):
        """
        Start a migration-destination VM.

        :param params: parameters of new VM, to be passed to
            *:meth:* - `~clientIF.create`.
        :type params: dict
        :param incomingLimit: maximum number of incoming migrations to set
            before the migration is started. Must be > 0.
        :type incomingLimit: int
        """
        self.log.debug('Migration create')

        if incomingLimit:
            self.log.debug('Setting incoming migration limit to %s',
                           incomingLimit)
            migration.incomingMigrations.bound = incomingLimit

        params['vmId'] = self._UUID
        result = self.create(params)
        if result['status']['code']:
            self.log.debug('Migration create - Failed')
            # for compatibility with < 4.0 src that could not handle the
            # retry error code
            is_old_source = incomingLimit is None
            is_retry_error = response.is_error(result, 'migrateLimit')
            if is_old_source and is_retry_error:
                self.log.debug('Returning backwards compatible migration '
                               'error code')
                return response.error('migrateErr')
            return result

        v = self._cif.vmContainer.get(self._UUID)

        try:
            if not v.waitForMigrationDestinationPrepare():
                return errCode['createErr']
        except exception.HookError as e:
            self.log.debug('Destination VM creation failed due to hook' +
                           ' error:' + str(e))
            return response.error('hookError', 'Destination hook failed: ' +
                                  str(e))
        self.log.debug('Destination VM creation succeeded')
        return {'status': doneCode, 'migrationPort': 0,
                'params': result['vmList']}

    def diskReplicateStart(self, srcDisk, dstDisk):
        v = self._cif.vmContainer.get(self._UUID)
        if not v:
            return errCode['noVM']
        return v.diskReplicateStart(srcDisk, dstDisk)

    def diskReplicateFinish(self, srcDisk, dstDisk):
        v = self._cif.vmContainer.get(self._UUID)
        if not v:
            return errCode['noVM']
        return v.diskReplicateFinish(srcDisk, dstDisk)

    def diskSizeExtend(self, driveSpecs, newSize):
        if self._UUID == VM.BLANK_UUID:
            try:
                volume = Volume(
                    driveSpecs['volumeID'], driveSpecs['poolID'],
                    driveSpecs['domainID'], driveSpecs['imageID'])
            except KeyError:
                return errCode['imageErr']
            return volume.updateSize(newSize)
        else:
            v = self._cif.vmContainer.get(self._UUID)
            if not v:
                return errCode['noVM']
            return v.diskSizeExtend(driveSpecs, newSize)

    def pause(self):
        v = self._cif.vmContainer.get(self._UUID)
        if not v:
            return errCode['noVM']
        return v.pause()

    def reset(self):
        """
        Press the virtual reset button for the specified VM.
        """
        return errCode['noimpl']

    def setTicket(self, password, ttl, existingConnAction, params):
        """
        Set the ticket (password) to be used to connect to a VM display

        :param vmId: specify the VM whos ticket is to be changed.
        :param password: new password
        :type password: string
        :param ttl: ticket lifetime (seconds)
        :param existingConnAction: what to do with a currently-connected
                client (SPICE only):
                ``disconnect`` - disconnect old client when a new client
                                 connects.
                ``keep``       - allow existing client to remain
                                 connected.
                ``fail``       - abort command without disconnecting
                                 the current client.
        :param additional parameters in dict format
        """
        try:
            v = self._cif.vmContainer[self._UUID]
        except KeyError:
            return errCode['noVM']
        return v.setTicket(password, ttl, existingConnAction, params)

    def shutdown(self, delay=None, message=None, reboot=False, timeout=None,
                 force=False):
        """
        Shut a VM down politely.

        :param message: message to be shown to guest user before shutting down
                        his machine.
        :param delay: grace period (seconds) to let guest user close his
                      applications.
        :param reboot: True if reboot is desired, False for shutdown
        :param timeout: number of seconds to wait before trying next
                        shutdown/reboot method
        :param force: True if shutdown/reboot desired by any means necessary
                      (forceful reboot/shutdown if all graceful methods fail)
        """
        try:
            v = self._cif.vmContainer[self._UUID]
        except KeyError:
            return errCode['noVM']
        if not delay:
            delay = config.get('vars', 'user_shutdown_timeout')
        if not message:
            message = USER_SHUTDOWN_MESSAGE
        if not timeout:
            timeout = config.getint('vars', 'sys_shutdown_timeout')
        return v.shutdown(delay, message, reboot, timeout, force)

    def _createSysprepFloppyFromInf(self, infFileBinary, floppyImage):
        try:
            rc, out, err = commands.execCmd([constants.EXT_MK_SYSPREP_FLOPPY,
                                             floppyImage],
                                            sudo=True,
                                            data=infFileBinary.data)
            if rc:
                return False
            else:
                return True
        except:
            self.log.error("Error creating sysprep floppy", exc_info=True)
            return False

    def _getHibernationPaths(self, hiberVolHandle):
        """
        Break *hiberVolHandle* into the "quartets" of hibernation images.
        """
        domainID, poolID, stateImageID, stateVolumeID, \
            paramImageID, paramVolumeID = hiberVolHandle.split(',')

        return dict(domainID=domainID, poolID=poolID, imageID=stateImageID,
                    volumeID=stateVolumeID, device='disk'), \
            dict(domainID=domainID, poolID=poolID,
                 imageID=paramImageID, volumeID=paramVolumeID,
                 device='disk')

    def freeze(self):
        v = self._cif.vmContainer.get(self._UUID)
        if not v:
            return errCode['noVM']
        return v.freeze()

    def thaw(self):
        v = self._cif.vmContainer.get(self._UUID)
        if not v:
            return errCode['noVM']
        return v.thaw()

    def snapshot(self, snapDrives, snapMemory=None, frozen=False):
        v = self._cif.vmContainer.get(self._UUID)
        if not v:
            return errCode['noVM']
        memoryParams = {}
        if snapMemory:
            memoryParams['dst'], memoryParams['dstparams'] = \
                self._getHibernationPaths(snapMemory)
        return v.snapshot(snapDrives, memoryParams, frozen=frozen)

    def setBalloonTarget(self, target):
        v = self._cif.vmContainer.get(self._UUID)
        if not v:
            return errCode['noVM']
        return v.setBalloonTarget(target)

    def setCpuTuneQuota(self, quota):
        v = self._cif.vmContainer.get(self._UUID)
        if not v:
            return errCode['noVM']
        return v.setCpuTuneQuota(quota)

    def getIoTune(self):
        v = self._cif.vmContainer.get(self._UUID)
        if not v:
            return errCode['noVM']
        return v.getIoTuneResponse()

    def setIoTune(self, tunables):
        v = self._cif.vmContainer.get(self._UUID)
        if not v:
            return errCode['noVM']
        return v.setIoTune(tunables)

    def getIoTunePolicy(self):
        v = self._cif.vmContainer.get(self._UUID)
        if not v:
            return errCode['noVM']
        return v.getIoTunePolicyResponse()

    def setCpuTunePeriod(self, period):
        v = self._cif.vmContainer.get(self._UUID)
        if not v:
            return errCode['noVM']
        return v.setCpuTunePeriod(period)

    def getDiskAlignment(self, disk):
        if self._UUID != VM.BLANK_UUID:
            return errCode['noimpl']
        return self._cif.getDiskAlignment(disk)

    def merge(self, drive, baseVolUUID, topVolUUID, bandwidth=0, jobUUID=None):
        v = self._cif.vmContainer.get(self._UUID)
        if not v:
            return errCode['noVM']
        return v.merge(drive, baseVolUUID, topVolUUID, bandwidth, jobUUID)


class Volume(APIBase):
    ctorArgs = ['volumeID', 'storagepoolID', 'storagedomainID', 'imageID']

    class Types:
        UNKNOWN = sc.UNKNOWN_VOL
        PREALLOCATED = sc.PREALLOCATED_VOL
        SPARSE = sc.SPARSE_VOL

    class Formats:
        UNKNOWN = sc.UNKNOWN_FORMAT
        COW = sc.COW_FORMAT
        RAW = sc.RAW_FORMAT

    class Roles:
        SHARED = sc.SHARED_VOL
        LEAF = sc.LEAF_VOL

    BLANK_UUID = sc.BLANK_UUID

    def __init__(self, UUID, spUUID, sdUUID, imgUUID):
        APIBase.__init__(self)
        self._UUID = UUID
        self._spUUID = spUUID
        self._sdUUID = sdUUID
        self._imgUUID = imgUUID

    def copy(self, dstSdUUID, dstImgUUID, dstVolUUID, desc, volType,
             volFormat, preallocate, postZero, force, discard=False):
        vmUUID = ''   # vmUUID is never used
        return self._irs.copyImage(self._sdUUID, self._spUUID, vmUUID,
                                   self._imgUUID, self._UUID, dstImgUUID,
                                   dstVolUUID, desc, dstSdUUID, volType,
                                   volFormat, preallocate, postZero, force,
                                   discard)

    def create(self, size, volFormat, preallocate, diskType, desc,
               srcImgUUID, srcVolUUID, initialSize=None):
        return self._irs.createVolume(self._sdUUID, self._spUUID,
                                      self._imgUUID, size, volFormat,
                                      preallocate, diskType, self._UUID, desc,
                                      srcImgUUID, srcVolUUID,
                                      initialSize=initialSize)

    def delete(self, postZero, force, discard=False):
        return self._irs.deleteVolume(self._sdUUID, self._spUUID,
                                      self._imgUUID, [self._UUID], postZero,
                                      force, discard)

    def verify_untrusted(self):
        return self._irs.verify_untrusted_volume(self._spUUID, self._sdUUID,
                                                 self._imgUUID, self._UUID)

    def extendSize(self, newSize):
        return self._irs.extendVolumeSize(
            self._spUUID, self._sdUUID, self._imgUUID, self._UUID, newSize)

    def updateSize(self, newSize):
        return self._irs.updateVolumeSize(
            self._spUUID, self._sdUUID, self._imgUUID, self._UUID, newSize)

    def getInfo(self):
        return self._irs.getVolumeInfo(self._sdUUID, self._spUUID,
                                       self._imgUUID, self._UUID)

    def getQemuImageInfo(self):
        return self._irs.getQemuImageInfo(self._sdUUID, self._spUUID,
                                          self._imgUUID, self._UUID)

    def getSize(self):
        return self._irs.getVolumeSize(self._sdUUID, self._spUUID,
                                       self._imgUUID, self._UUID)

    def setSize(self, newSize):
        return self._irs.setVolumeSize(self._sdUUID, self._spUUID,
                                       self._imgUUID, self._UUID, newSize)

    def refresh(self):
        return self._irs.refreshVolume(self._sdUUID, self._spUUID,
                                       self._imgUUID, self._UUID)

    def setDescription(self, description):
        return self._irs.setVolumeDescription(self._sdUUID, self._spUUID,
                                              self._imgUUID, self._UUID,
                                              description)

    def setLegality(self, legality):
        return self._irs.setVolumeLegality(self._sdUUID, self._spUUID,
                                           self._imgUUID, self._UUID, legality)


class Image(APIBase):
    ctorArgs = ['imageID', 'storagepoolID', 'storagedomainID']

    BLANK_UUID = sc.BLANK_UUID

    class DiskTypes:
        UNKNOWN = storage.image.UNKNOWN_DISK_TYPE
        SYSTEM = storage.image.SYSTEM_DISK_TYPE
        DATA = storage.image.DATA_DISK_TYPE
        SHARED = storage.image.SHARED_DISK_TYPE
        SWAP = storage.image.SWAP_DISK_TYPE
        TEMP = storage.image.TEMP_DISK_TYPE

    def __init__(self, UUID, spUUID, sdUUID):
        APIBase.__init__(self)
        self._UUID = UUID
        self._spUUID = spUUID
        self._sdUUID = sdUUID

    def delete(self, postZero, force, discard=False):
        return self._irs.deleteImage(self._sdUUID, self._spUUID, self._UUID,
                                     postZero, force, discard)

    def deleteVolumes(self, volumeList, postZero=False, force=False,
                      discard=False):
        return self._irs.deleteVolume(self._sdUUID, self._spUUID, self._UUID,
                                      volumeList, postZero, force, discard)

    def getVolumes(self):
        return self._irs.getVolumesList(self._sdUUID, self._spUUID, self._UUID)

    def mergeSnapshots(self, ancestor, successor, postZero, discard=False):
        vmUUID = ''   # Not used
        # XXX: On success, self._sdUUID needs to be updated
        return self._irs.mergeSnapshots(self._sdUUID, self._spUUID, vmUUID,
                                        self._UUID, ancestor, successor,
                                        postZero, discard)

    def move(self, dstSdUUID, operation, postZero, force, discard=False):
        vmUUID = ''   # Not used
        # XXX: On success, self._sdUUID needs to be updated
        return self._irs.moveImage(self._spUUID, self._sdUUID, dstSdUUID,
                                   self._UUID, vmUUID, operation, postZero,
                                   force, discard)

    def sparsify(self, tmpVolUUID, dstSdUUID, dstImgUUID, dstVolUUID):
        return self._irs.sparsifyImage(self._spUUID, self._sdUUID, self._UUID,
                                       tmpVolUUID, dstSdUUID, dstImgUUID,
                                       dstVolUUID)

    def cloneStructure(self, dstSdUUID):
        return self._irs.cloneImageStructure(self._spUUID, self._sdUUID,
                                             self._UUID, dstSdUUID)

    def syncData(self, dstSdUUID, syncType):
        return self._irs.syncImageData(self._spUUID, self._sdUUID, self._UUID,
                                       dstSdUUID, syncType)

    def upload(self, methodArgs, volumeID=None):
        return self._irs.uploadImage(
            methodArgs, self._spUUID, self._sdUUID, self._UUID, volumeID)

    def download(self, methodArgs, volumeID=None):
        return self._irs.downloadImage(
            methodArgs, self._spUUID, self._sdUUID, self._UUID, volumeID)

    def prepare(self, volumeID, allowIllegal=False):
        return self._irs.prepareImage(self._sdUUID, self._spUUID,
                                      self._UUID, volumeID,
                                      allowIllegal=allowIllegal)

    def teardown(self, volumeID=None):
        return self._irs.teardownImage(
            self._sdUUID, self._spUUID, self._UUID, volumeID)

    def uploadToStream(self, methodArgs, callback, startEvent, volUUID=None):
        return self._irs.uploadImageToStream(
            methodArgs, callback, startEvent, self._spUUID, self._sdUUID,
            self._UUID, volUUID)

    def downloadFromStream(self, methodArgs, callback, volUUID=None):
        return self._irs.downloadImageFromStream(
            methodArgs, callback, self._spUUID, self._sdUUID, self._UUID,
            volUUID)

    def reconcileVolumeChain(self, leafVolID):
        return self._irs.reconcileVolumeChain(self._spUUID, self._sdUUID,
                                              self._UUID, leafVolID)


class LVMVolumeGroup(APIBase):
    ctorArgs = ['lvmvolumegroupID']

    def __init__(self, lvmvolumegroupID=None):
        APIBase.__init__(self)
        self._UUID = lvmvolumegroupID

    def create(self, name, devlist, force=False):
        return self._irs.createVG(name, devlist, force)

    def getInfo(self):
        if self._UUID is not None:
            return self._irs.getVGInfo(self._UUID)
        else:
            # FIXME: Add proper error return
            return None

    def remove(self):
        if self._UUID is not None:
            return self._irs.removeVG(self._UUID)
        else:
            # FIXME: Add proper error return
            return None


class ISCSIConnection(APIBase):
    ctorArgs = ['host', 'port', 'user', 'password', 'ipv6_enabled']

    def __init__(self, host, port, user="", password="", ipv6_enabled=False):
        APIBase.__init__(self)
        self._host = host
        self._port = port
        self._user = user
        self._pass = password
        self._ipv6_enabled = ipv6_enabled

    def discoverSendTargets(self):
        params = {'connection': self._host, 'port': self._port,
                  'user': self._user, 'password': self._pass,
                  'ipv6_enabled': self._ipv6_enabled}
        return self._irs.discoverSendTargets(params)


class StorageDomain(APIBase):
    ctorArgs = ['storagedomainID']

    class Types:
        UNKNOWN = storage.sd.UNKNOWN_DOMAIN
        NFS = storage.sd.NFS_DOMAIN
        FCP = storage.sd.FCP_DOMAIN
        ISCSI = storage.sd.ISCSI_DOMAIN
        LOCALFS = storage.sd.LOCALFS_DOMAIN
        CIFS = storage.sd.CIFS_DOMAIN
        POSIXFS = storage.sd.POSIXFS_DOMAIN

    class Classes:
        DATA = storage.sd.DATA_DOMAIN
        ISO = storage.sd.ISO_DOMAIN
        BACKUP = storage.sd.BACKUP_DOMAIN

    BLANK_UUID = storage.sd.BLANK_UUID

    def __init__(self, UUID):
        APIBase.__init__(self)
        self._UUID = UUID

    def activate(self, storagepoolID):
        return self._irs.activateStorageDomain(self._UUID, storagepoolID)

    def attach(self, storagepoolID):
        return self._irs.attachStorageDomain(self._UUID, storagepoolID)

    def create(self, domainType, typeArgs, name, domainClass, version=None):
        if version is None:
            version = constants.SUPPORTED_DOMAIN_VERSIONS[0]
        return self._irs.createStorageDomain(domainType, self._UUID, name,
                                             typeArgs, domainClass, version)

    def deactivate(self, storagepoolID, masterSdUUID, masterVersion):
        return self._irs.deactivateStorageDomain(self._UUID, storagepoolID,
                                                 masterSdUUID, masterVersion)

    def detach(self, storagepoolID, masterSdUUID=None, masterVersion=0,
               force=False):
        if force:
            return self._irs.forcedDetachStorageDomain(self._UUID,
                                                       storagepoolID)
        else:
            return self._irs.detachStorageDomain(self._UUID, storagepoolID,
                                                 masterSdUUID, masterVersion)

    def extend(self, storagepoolID, devlist, force=False):
        return self._irs.extendStorageDomain(self._UUID, storagepoolID,
                                             devlist, force)

    def resizePV(self, storagepoolID, guid):
        return self._irs.resizePV(self._UUID, storagepoolID, guid)

    def format(self, autoDetach):
        return self._irs.formatStorageDomain(self._UUID, autoDetach)

    def getFileStats(self, pattern, caseSensitive):
        return self._irs.getFileStats(self._UUID, pattern, caseSensitive)

    def getImages(self):
        return self._irs.getImagesList(self._UUID)

    def getInfo(self):
        return self._irs.getStorageDomainInfo(self._UUID)

    def getStats(self):
        return self._irs.getStorageDomainStats(self._UUID)

    def getVolumes(self, storagepoolID, imageID=Image.BLANK_UUID):
        return self._irs.getVolumesList(self._UUID, storagepoolID, imageID)

    def setDescription(self, description):
        return self._irs.setStorageDomainDescription(self._UUID, description)

    def validate(self):
        return self._irs.validateStorageDomain(self._UUID)


class StoragePool(APIBase):
    ctorArgs = ['storagepoolID']

    def __init__(self, UUID):
        APIBase.__init__(self)
        self._UUID = UUID

    # scsiKey not used
    def connect(self, hostID, scsiKey, masterSdUUID, masterVersion,
                domainDict=None):
        return self._irs.connectStoragePool(
            self._UUID, hostID, masterSdUUID, masterVersion, domainDict)

    def connectStorageServer(self, domainType, connectionParams):
        return self._irs.connectStorageServer(domainType, self._UUID,
                                              connectionParams)

    def create(self, name, masterSdUUID, masterVersion, domainList,
               lockRenewalIntervalSec, leaseTimeSec, ioOpTimeoutSec,
               leaseRetries):
        poolType = None   # Not used
        lockPolicy = None   # Not used
        return self._irs.createStoragePool(
            poolType, self._UUID, name, masterSdUUID, domainList,
            masterVersion, lockPolicy, lockRenewalIntervalSec, leaseTimeSec,
            ioOpTimeoutSec, leaseRetries)

    # scsiKey not used
    def destroy(self, hostID, scsiKey):
        return self._irs.destroyStoragePool(self._UUID, hostID)

    # scsiKey not used
    def disconnect(self, hostID, scsiKey, remove=False):
        return self._irs.disconnectStoragePool(self._UUID, hostID, remove)

    def disconnectStorageServer(self, domainType, connectionParams):
        return self._irs.disconnectStorageServer(domainType, self._UUID,
                                                 connectionParams)

    def fence(self):
        lastOwner = None   # Unused
        lastLver = None   # Unused
        return self._irs.fenceSpmStorage(self._UUID, lastOwner, lastLver)

    def getBackedUpVmsInfo(self, storagedomainID, vmList):
        return self._irs.getVmsInfo(self._UUID, storagedomainID, vmList)

    def getBackedUpVmsList(self, storagedomainID):
        return self._irs.getVmsList(self._UUID, storagedomainID)

    def getDomainsContainingImage(self, imageID):
        return self._irs.getImageDomainsList(self._UUID, imageID)

    def getSpmStatus(self):
        return self._irs.getSpmStatus(self._UUID)

    def getInfo(self):
        return self._irs.getStoragePoolInfo(self._UUID)

    def moveMultipleImages(self, srcSdUUID, dstSdUUID, imgDict,
                           force=False):
        vmUUID = None   # Unused parameter
        return self._irs.moveMultipleImages(self._UUID, srcSdUUID, dstSdUUID,
                                            imgDict, vmUUID, force)

    def reconstructMaster(self, hostId, name, masterSdUUID, masterVersion,
                          domainDict, lockRenewalIntervalSec, leaseTimeSec,
                          ioOpTimeoutSec, leaseRetries):
        lockPolicy = None   # Not used
        return self._irs.reconstructMaster(
            self._UUID, name, masterSdUUID, domainDict, masterVersion,
            lockPolicy, lockRenewalIntervalSec, leaseTimeSec, ioOpTimeoutSec,
            leaseRetries, hostId)

    def setDescription(self, description):
        return self._irs.setStoragePoolDescription(self._UUID, description)

    def spmStart(self, prevID, prevLver, enableScsiFencing,
                 maxHostID=None, domVersion=None):
        if maxHostID is None:
            maxHostID = clusterlock.MAX_HOST_ID
        return self._irs.spmStart(self._UUID, prevID, prevLver, maxHostID,
                                  domVersion)

    def spmStop(self):
        return self._irs.spmStop(self._UUID)

    def upgrade(self, targetDomVersion):
        return self._irs.upgradeStoragePool(self._UUID, targetDomVersion)

    def updateVMs(self, vmList, storagedomainID=None):
        return self._irs.updateVM(self._UUID, vmList, storagedomainID)

    def removeVM(self, vmUUID, storagedomainID=None):
        return self._irs.removeVM(self._UUID, vmUUID, storagedomainID)

    def prepareMerge(self, subchainInfo):
        return self._irs.prepareMerge(self._UUID, subchainInfo)

    def finalizeMerge(self, subchainInfo):
        return self._irs.finalizeMerge(self._UUID, subchainInfo)


class Global(APIBase):
    ctorArgs = []

    def __init__(self):
        APIBase.__init__(self)

    # General Host functions
    def fenceNode(self, addr, port, agent, username, password, action,
                  secure=False, options='', policy=None):
        """Send a fencing command to a remote node.

           agent is one of (rsa, ilo, drac5, ipmilan, etc)
           action can be one of (status, on, off, reboot)."""

        def fence(script, inp):
            rc, out, err = commands.execCmd([script], data=inp)
            self.log.debug('rc %s inp %s out %s err %s', rc,
                           hidePasswd(inp), out, err)
            return rc, out, err

        def hidePasswd(text):
            cleantext = ''
            for line in text.splitlines(True):
                if line.startswith('passwd='):
                    line = 'passwd=XXXX\n'
                cleantext += line
            return cleantext

        def should_fence(policy):
            # Skip fencing if any of the condition mentioned in the fencing
            # policy is met
            result = False
            if policy is None:
                self.log.debug('No policy specified')
                return True
            result = check_virt_fencing_policies(policy)
            if result:
                result = check_gluster_fencing_policies(policy)
            return result

        def check_virt_fencing_policies(policy):
            # skip fence execution if map of storage domains with host id is
            # entered and at least one storage domain connection from host is
            # alive. Also enforce the following gluster related fencing
            # policies.
            hostIdMap = policy.get('storageDomainHostIdMap')
            if not hostIdMap:
                self.log.warning('No storageDomainHostIdMap provided')
                return True

            result = self._irs.getHostLeaseStatus(hostIdMap)
            if result['status']['code'] != 0:
                self.log.error(
                    "Error getting host lease status, error code '%s'",
                    result['status']['code'])
                return True

            # HOST_STATUS_LIVE means that host renewed its lease in last 80
            # seconds. If so, we consider the host Up and we won't execute
            # fencing, even when it's unreachable from engine
            for sd, status in result['domains'].iteritems():
                if status == clusterlock.HOST_STATUS_LIVE:
                    self.log.debug("Host has live lease on '%s'", sd)
                    return False

            self.log.debug("Host doesn't have any live lease")
            return True

        def check_gluster_fencing_policies(policy):
            # If skipFencingIfGlusterBricksUp is set to true the fencing should
            # should be skipped if there is any brick up running in the host
            # being fenced.
            skipFencingIfGlusterBricksUp = \
                policy.get('skipFencingIfGlusterBricksUp')
            # If skipFencingIfGlusterQuorumNotMet is set to true then fencing
            # should be skipped if the gluster bricks are UP and fencing
            # this host will bring down those bricks and quourm will be
            # lost for any replicated volume in the gluster.
            skipFencingIfGlusterQuorumNotMet = \
                policy.get('skipFencingIfGlusterQuorumNotMet')
            hostUuid = policy.get('glusterServerUuid')
            if skipFencingIfGlusterBricksUp \
                    or skipFencingIfGlusterQuorumNotMet:
                if not glusterFence:
                    self.log.error("Required vdsm-gluster package is "
                                   "missing on this host. Note that "
                                   "gluster related fencing will not be"
                                   "enforced!. Please install the missing "
                                   "package in order to enforce gluster "
                                   "related fencing polices")
                    return True
                result, msg = glusterFence. \
                    can_fence_host(supervdsm.getProxy(), hostUuid,
                                   skipFencingIfGlusterBricksUp,
                                   skipFencingIfGlusterQuorumNotMet)

                self.log.debug(msg)
                return result

            return True

        self.log.debug('fenceNode(addr=%s,port=%s,agent=%s,user=%s,passwd=%s,'
                       'action=%s,secure=%s,options=%s,policy=%s)',
                       addr, port, agent, username, password, action, secure,
                       options, policy)

        if action not in ('status', 'on', 'off', 'reboot'):
            raise ValueError('illegal action ' + action)

        if action != 'status' and not should_fence(policy):
            self.log.debug("Skipping execution of action '%s'", action)
            return {'status': doneCode, 'operationStatus': 'skipped'}

        script = constants.EXT_FENCE_PREFIX + agent

        inp = ('agent=fence_%s\nipaddr=%s\nlogin=%s\naction=%s\n'
               'passwd=%s\n') % (agent, addr, username, action, password.value)
        if port != '':
            inp += 'port=%s\n' % (port,)
        if utils.tobool(secure):
            inp += 'secure=yes\n'
        inp += options

        try:
            rc, out, err = fence(script, inp)
        except OSError as e:
            if e.errno == os.errno.ENOENT:
                return errCode['fenceAgent']
            raise
        self.log.debug('rc %s in %s out %s err %s', rc,
                       hidePasswd(inp), out, err)
        if not 0 <= rc <= 2:
            return {'status': {'code': 1,
                               'message': out + err}}
        message = doneCode['message']
        ret = 0
        if action == 'status':
            if rc == 0:
                power = 'on'
            elif rc == 2:
                power = 'off'
            else:
                power = 'unknown'
                message = out + err
                ret = rc
            return {'status': {'code': ret, 'message': message},
                    'power': power}
        if rc != 0:
            message = out + err
        return {'status': {'code': rc, 'message': message},
                'power': 'unknown', 'operationStatus': 'initiated'}

    def ping(self):
        "Ping the server. Useful for tests"
        updateTimestamp()
        return {'status': doneCode}

    def getCapabilities(self):
        """
        Report host capabilities.
        """
        hooks.before_get_caps()
        updateTimestamp()  # required for some ovirt-3.0.z Engines
        c = caps.get()
        c['netConfigDirty'] = str(self._cif._netConfigDirty)
        c = hooks.after_get_caps(c)

        return {'status': doneCode, 'info': c}

    def getHardwareInfo(self):
        """
        Report host hardware information
        """
        try:
            hw = supervdsm.getProxy().getHardwareInfo()
            return {'status': doneCode, 'info': hw}
        except:
            self.log.error("failed to retrieve hardware info", exc_info=True)
            return errCode['hwInfoErr']

    def getAllVmStats(self):
        """
        Get statistics of all running VMs.
        """
        hooks.before_get_all_vm_stats()
        statsList = self._cif.getAllVmStats()
        statsList = hooks.after_get_all_vm_stats(statsList)
        throttledlog.info('getAllVmStats', "Current getAllVmStats: %s",
                          AllVmStatsValue(statsList))
        return {'status': doneCode, 'statsList': Suppressed(statsList)}

    def getAllVmIoTunePolicies(self):
        """
        Get IO tuning policies of all running VMs.
        """
        io_tune_policies_dict = self._cif.getAllVmIoTunePolicies()
        return {'status': doneCode,
                'io_tune_policies_dict': io_tune_policies_dict}

    def hostdevListByCaps(self, caps=None):
        devices = hostdev.list_by_caps(caps)
        return {'status': doneCode, 'deviceList': devices}

    def hostdevChangeNumvfs(self, deviceName, numvfs):
        self._cif._netConfigDirty = True
        hostdev.change_numvfs(deviceName, numvfs)
        return {'status': doneCode}

    def hostdevReattach(self, deviceName):
        hostdev.reattach_detachable(deviceName)
        return {'status': doneCode}

    def getStats(self):
        """
        Report host statistics.
        """
        return {'status': doneCode,
                'info': hostapi.get_stats(self._cif,
                                          sampling.host_samples.stats())}

    def setLogLevel(self, level, name=''):
        """
        Set verbosity level of vdsm's log.
        Doesn't survive a restart.

        :param level: requested logging level.
                Examples: `logging.DEBUG` `logging.ERROR`
        :type level: string
        :param name: logger name to set. If not provided,
                defaults to the root logger.
                Otherwise, tune the specific logger provided.
        :type name: string
        """
        logUtils.set_level(level, name)
        return dict(status=doneCode)

    # VM-related functions
    def getVMList(self, fullStatus=False, vmList=(), onlyUUID=False):
        """ return a list of known VMs with full (or partial) config each """
        # To improve complexity, convert 'vms' to set(vms)
        vmSet = set(vmList)
        vmlist = [v.status(fullStatus)
                  for v in self._cif.vmContainer.values()
                  if not vmSet or v.id in vmSet]
        if not fullStatus and onlyUUID:
            # BZ 1196735: api backward compatibility issue
            # REQUIRED_FOR: engine-3.5.0 only
            vmlist = [v['vmId'] for v in vmlist]
        return {'status': doneCode, 'vmList': vmlist}

    def getExternalVMs(self, uri, username, password, vm_names=None):
        """
        Return information about the not-KVM virtual machines:
        getExternalVMs returns list of VMs with subsection of  properties
        that returns from getVmsList (with the same keys ie vmName for name)
        currently v2v returns the following information:
            vm: vmName, vmId, state, memSize, smp, disks and network list,
            disk: dev, alias
            network: type, macAddr, bridge, dev
        """
        return v2v.get_external_vms(uri, username, password, vm_names)

    def getExternalVMNames(self, uri, username, password):
        """
        Return names of VMs running on external hypervisor.
        """
        return v2v.get_external_vm_names(uri, username, password)

    def getExternalVmFromOva(self, ova_path):
        """
        Return information regarding a VM that is a part of the ova:
        getExternalVmFromOva return information on a VM that is a part
        of the provided ova file.
        The return value is a VM with the following information:
            vm: vmName, state, memSize, smp, disks and network list,
            disk: type, capacity, alias, allocation
            network: dev, model, type, bridge
        """
        return v2v.get_ova_info(ova_path)

    def convertExternalVm(self, uri, username, password, vminfo, jobid):
        return v2v.convert_external_vm(uri, username, password, vminfo, jobid,
                                       self._irs)

    def convertExternalVmFromOva(self, ova_path, vminfo, jobid):
        return v2v.convert_ova(ova_path, vminfo, jobid, self._cif.irs)

    def getJobs(self, job_type=None, job_ids=()):
        found = jobs.info(job_type=job_type, job_ids=job_ids)
        return response.success(jobs=found)

    def getConvertedVm(self, jobid):
        return v2v.get_converted_vm(jobid)

    def deleteV2VJob(self, jobid):
        return v2v.delete_job(jobid)

    def abortV2VJob(self, jobid):
        return v2v.abort_job(jobid)

    def registerSecrets(self, secrets, clear=False):
        return secret.register(secrets, clear=clear)

    def unregisterSecrets(self, uuids):
        return secret.unregister(uuids)

    # Networking-related functions
    def setupNetworks(self, networks, bondings, options):
        """Add a new network to this vds, replacing an old one."""

        if not self._cif._networkSemaphore.acquire(blocking=False):
            self.log.warn('concurrent network verb already executing')
            return errCode['unavail']

        try:
            self._cif._netConfigDirty = True
            supervdsm.getProxy().setupNetworks(networks, bondings, options)
            return {'status': doneCode}
        except ConfigNetworkError as e:
            self.log.error(e.message, exc_info=True)
            return {'status': {'code': e.errCode, 'message': e.message}}
        except exception.HookError as e:
            return response.error('hookError', 'Hook error: ' + str(e))
        except:
            raise
        finally:
            self._cif._networkSemaphore.release()

    def setSafeNetworkConfig(self):
        """Declare current network configuration as 'safe'"""
        if not self._cif._networkSemaphore.acquire(blocking=False):
            self.log.warn('concurrent network verb already executing')
            return errCode['unavail']
        try:
            self._cif._netConfigDirty = False
            supervdsm.getProxy().setSafeNetworkConfig()
            return {'status': doneCode}
        finally:
            self._cif._networkSemaphore.release()

    # Top-level storage functions
    def getStorageDomains(self, storagepoolID=None, domainClass=None,
                          storageType=None, remotePath=None):
        return self._irs.getStorageDomainsList(storagepoolID, domainClass,
                                               storageType, remotePath)

    def getConnectedStoragePools(self):
        return self._irs.getConnectedStoragePoolsList()

    def getStorageRepoStats(self):
        return self._irs.repoStats()

    def startMonitoringDomain(self, sdUUID, hostID):
        return self._irs.startMonitoringDomain(sdUUID, hostID)

    def stopMonitoringDomain(self, sdUUID):
        return self._irs.stopMonitoringDomain(sdUUID)

    def getLVMVolumeGroups(self, storageType=None):
        return self._irs.getVGList(storageType)

    def getDeviceList(self, storageType=None, guids=(), checkStatus=True):
        return self._irs.getDeviceList(storageType, guids, checkStatus)

    def getDevicesVisibility(self, guidList):
        return self._irs.getDevicesVisibility(guidList)

    def getAllTasksInfo(self):
        return self._irs.getAllTasksInfo()

    def getAllTasksStatuses(self):
        return self._irs.getAllTasksStatuses()

    def getAllTasks(self):
        return self._irs.getAllTasks()

    def setMOMPolicy(self, policy):
        try:
            self._cif.mom.setPolicy(policy)
            return dict(status=doneCode)
        except:
            return errCode['momErr']

    def setMOMPolicyParameters(self, key_value_store):
        try:
            self._cif.mom.setPolicyParameters(key_value_store)
            return dict(status=doneCode)
        except:
            return errCode['momErr']

    def setKsmTune(self, tuningParams):
        try:
            supervdsm.getProxy().ksmTune(tuningParams)
            return dict(status=doneCode)
        except:
            self.log.exception('setKsmTune API call failed.')
            return errCode['ksmErr']

    def setHaMaintenanceMode(self, mode, enabled):
        """
        Sets Hosted Engine HA maintenance mode ('global' or 'local') to
        enabled (True) or disabled (False).
        """
        if not haClient:
            return errCode['unavail']

        self.log.info("Setting Hosted Engine HA %s maintenance to %s",
                      mode.lower(), enabled)
        if mode.lower() == 'global':
            mm = haClient.HAClient.MaintenanceMode.GLOBAL
        elif mode.lower() == 'local':
            mm = haClient.HAClient.MaintenanceMode.LOCAL
        else:
            return errCode['haErr']

        try:
            haClient.HAClient().set_maintenance_mode(mm, enabled)
        except Exception:
            self.log.exception("error setting HA maintenance mode")
            return errCode['haErr']
        return {'status': doneCode}

    def add_image_ticket(self, ticket):
        return self._irs.add_image_ticket(ticket)

    def remove_image_ticket(self, uuid):
        return self._irs.remove_image_ticket(uuid)

    def extend_image_ticket(self, uuid, timeout):
        return self._irs.extend_image_ticket(uuid, timeout)


class SDM(APIBase):
    ctorArgs = []

    def create_volume(self, job_id, vol_info):
        return self._irs.sdm_create_volume(job_id, vol_info)

    def copy_data(self, job_id, source, destination):
        return self._irs.sdm_copy_data(job_id, source, destination)

    def sparsify_volume(self, job_id, vol_info):
        return self._irs.sdm_sparsify_volume(job_id, vol_info)

    def amend_volume(self, job_id, vol_info, qcow2_attr):
        return self._irs.sdm_amend_volume(job_id, vol_info, qcow2_attr)

    def merge(self, job_id, subchain_info):
        return self._irs.sdm_merge(job_id, subchain_info)

    def move_domain_device(self, job_id, move_params):
        return self._irs.sdm_move_domain_device(job_id, move_params)

    def reduce_domain(self, job_id, reduce_params):
        return self._irs.sdm_reduce_domain(job_id, reduce_params)

    def set_volume_generation(self, job_id, vol_info, new_gen):
        return self._irs.sdm_set_volume_generation(job_id, vol_info, new_gen)

    def update_volume(self, job_id, vol_info, vol_attr):
        return self._irs.sdm_update_volume(job_id, vol_info, vol_attr)


class Lease(APIBase):
    ctorArgs = []

    def create(self, lease):
        return self._irs.create_lease(lease)

    def delete(self, lease):
        return self._irs.delete_lease(lease)

    def info(self, lease):
        return self._irs.lease_info(lease)

    def status(self, lease):
        return self._irs.lease_status(lease)

    def rebuild_leases(self, sd_id):
        return self._irs.rebuild_leases(sd_id)
