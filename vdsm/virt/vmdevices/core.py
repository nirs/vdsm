#
# Copyright 2014 Red Hat, Inc.
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

import os.path

from vdsm.host import rngsources
from vdsm import constants
from vdsm import supervdsm
from vdsm import utils
from vdsm.virt.utils import cleanup_guest_socket

from . import hwclass
from .. import vmxml


class SkipDevice(Exception):
    pass


class Base(vmxml.Device):
    __slots__ = ('deviceType', 'device', 'alias', 'specParams', 'deviceId',
                 'conf', 'log', '_deviceXML', 'type', 'custom',
                 'is_hostdevice')

    def __init__(self, conf, log, **kwargs):
        self.conf = conf
        self.log = log
        self.specParams = {}
        self.custom = kwargs.pop('custom', {})
        for attr, value in kwargs.iteritems():
            try:
                setattr(self, attr, value)
            except AttributeError:  # skip read-only properties
                self.log.debug('Ignoring param (%s, %s) in %s', attr, value,
                               self.__class__.__name__)
        self._deviceXML = None
        self.is_hostdevice = False

    def __str__(self):
        attrs = [':'.join((a, str(getattr(self, a, None)))) for a in dir(self)
                 if not a.startswith('__')]
        return ' '.join(attrs)

    def is_attached_to(self, xml_string):
        raise NotImplementedError(
            "%s does not implement is_attached_to", self.__class__.__name__)

    @classmethod
    def update_device_info(cls, vm, device_conf):
        """
        Obtain info about this class of devices from libvirt domain and update
        the corresponding device structures.

        :param vm: VM for which the device info should be updated
        :type vm: `class:Vm` instance
        :param device_conf: VM device configuration corresponding to the given
          device.
        :type device_conf: list of dictionaries

        """
        raise NotImplementedError()

    def setup(self):
        """
        Actions to be executed before VM is started. This method is therefore
        able to modify the final device XML. Not executed in the recovery
        flow.

        It is implementation's obligation to
        * fail without leaving the device in inconsistent state or
        * succeed fully.

        In case of failure, teardown will not be called for device where setup
        failed, only for the devices that were successfully setup before
        the failure.
        """
        pass

    def teardown(self):
        """
        Actions to be executed after VM is destroyed.
        """
        pass


class Generic(Base):

    def getXML(self):
        """
        Create domxml for general device
        """
        return self.createXmlElem(self.type, self.device, ['address'])


class Balloon(Base):
    __slots__ = ('address',)

    def getXML(self):
        """
        Create domxml for a memory balloon device.

        <memballoon model='virtio'>
          <address type='pci' domain='0x0000' bus='0x00' slot='0x04'
           function='0x0'/>
        </memballoon>
        """
        m = self.createXmlElem(self.device, None, ['address'])
        m.setAttrs(model=self.specParams['model'])
        return m

    @classmethod
    def update_device_info(cls, vm, device_conf):
        for x in vm.domain.get_device_elements('memballoon'):
            # Ignore balloon devices without address.
            if vmxml.find_first(x, 'address', None) is None:
                address = None
            else:
                address = vmxml.device_address(x)
            alias = vmxml.find_attr(x, 'alias', 'name')

            for dev in device_conf:
                if address and not hasattr(dev, 'address'):
                    dev.address = address
                if alias and not hasattr(dev, 'alias'):
                    dev.alias = alias

            for dev in vm.conf['devices']:
                if dev['type'] == hwclass.BALLOON:
                    if address and not dev.get('address'):
                        dev['address'] = address
                    if alias and not dev.get('alias'):
                        dev['alias'] = alias


class Console(Base):
    __slots__ = ('_path',)

    CONSOLE_EXTENSION = '.sock'

    def __init__(self, *args, **kwargs):
        super(Console, self).__init__(*args, **kwargs)
        if not hasattr(self, 'specParams'):
            self.specParams = {}

        if utils.tobool(self.specParams.get('enableSocket', False)):
            self._path = os.path.join(
                constants.P_OVIRT_VMCONSOLES,
                self.conf['vmId'] + self.CONSOLE_EXTENSION
            )
        else:
            self._path = None

    def prepare(self):
        if self._path:
            supervdsm.getProxy().prepareVmChannel(
                self._path,
                constants.OVIRT_VMCONSOLE_GROUP)

    def cleanup(self):
        if self._path:
            cleanup_guest_socket(self._path)

    @property
    def isSerial(self):
        return self.specParams.get('consoleType', 'virtio') == 'serial'

    def getSerialDeviceXML(self):
        """
        Add a serial port for the console device if it exists and is a
        'serial' type device.

        <serial type='pty'>
            <target port='0'>
        </serial>

        or

        <serial type='unix'>
            <source mode='bind'
              path='/var/run/ovirt-vmconsole-console/${VMID}.sock'/>
            <target port='0'/>
        </serial>
        """
        if self._path:
            s = self.createXmlElem('serial', 'unix')
            s.appendChildWithArgs('source', mode='bind', path=self._path)
        else:
            s = self.createXmlElem('serial', 'pty')
        s.appendChildWithArgs('target', port='0')
        return s

    def getXML(self):
        """
        Create domxml for a console device.

        <console type='pty'>
          <target type='serial' port='0'/>
        </console>

        or:

        <console type='pty'>
          <target type='virtio' port='0'/>
        </console>

        or

        <console type='unix'>
          <source mode='bind' path='/path/to/${vmid}.sock'>
          <target type='virtio' port='0'/>
        </console>
        """
        if self._path:
            m = self.createXmlElem('console', 'unix')
            m.appendChildWithArgs('source', mode='bind', path=self._path)
        else:
            m = self.createXmlElem('console', 'pty')
        consoleType = self.specParams.get('consoleType', 'virtio')
        m.appendChildWithArgs('target', type=consoleType, port='0')
        return m

    @classmethod
    def update_device_info(cls, vm, device_conf):
        for x in vm.domain.get_device_elements('console'):
            # All we care about is the alias
            alias = vmxml.find_attr(x, 'alias', 'name')
            for dev in device_conf:
                if not hasattr(dev, 'alias'):
                    dev.alias = alias

            for dev in vm.conf['devices']:
                if dev['device'] == hwclass.CONSOLE and \
                        not dev.get('alias'):
                    dev['alias'] = alias


class Controller(Base):
    __slots__ = ('address', 'model', 'index', 'master')

    def getXML(self):
        """
        Create domxml for controller device
        """
        ctrl = self.createXmlElem('controller', self.device,
                                  ['index', 'model', 'master', 'address'])
        if self.device == 'virtio-serial':
            ctrl.setAttrs(index='0', ports='16')

        iothread = self.specParams.get('ioThreadId', None)
        if iothread is not None:
            ctrl.appendChildWithArgs('driver', iothread=iothread)

        return ctrl

    @classmethod
    def update_device_info(cls, vm, device_conf):
        for x in vm.domain.get_device_elements('controller'):
            # Ignore controller devices without address
            if vmxml.find_first(x, 'address', None) is None:
                continue
            alias = vmxml.find_attr(x, 'alias', 'name')
            device = vmxml.attr(x, 'type')
            # Get model and index. Relevant for USB controllers.
            model = vmxml.attr(x, 'model')
            index = vmxml.attr(x, 'index')

            # Get controller address
            address = vmxml.device_address(x)

            # In case the controller has index and/or model, they
            # are compared. Currently relevant for USB controllers.
            for ctrl in device_conf:
                if ((ctrl.device == device) and
                        (not hasattr(ctrl, 'index') or ctrl.index == index) and
                        (not hasattr(ctrl, 'model') or ctrl.model == model)):
                    ctrl.alias = alias
                    ctrl.address = address
            # Update vm's conf with address for known controller devices
            # In case the controller has index and/or model, they
            # are compared. Currently relevant for USB controllers.
            knownDev = False
            for dev in vm.conf['devices']:
                if ((dev['type'] == hwclass.CONTROLLER) and
                        (dev['device'] == device) and
                        ('index' not in dev or dev['index'] == index) and
                        ('model' not in dev or dev['model'] == model)):
                    dev['address'] = address
                    dev['alias'] = alias
                    knownDev = True
            # Add unknown controller device to vm's conf
            if not knownDev:
                vm.conf['devices'].append(
                    {'type': hwclass.CONTROLLER,
                     'device': device,
                     'address': address,
                     'alias': alias})


class Smartcard(Base):
    __slots__ = ('address',)

    def getXML(self):
        """
        Add smartcard section to domain xml

        <smartcard mode='passthrough' type='spicevmc'>
          <address ... />
        </smartcard>
        """
        card = self.createXmlElem(self.device, None, ['address'])
        sourceAttrs = {'mode': self.specParams['mode']}
        if sourceAttrs['mode'] != 'host':
            sourceAttrs['type'] = self.specParams['type']
        card.setAttrs(**sourceAttrs)
        return card

    @classmethod
    def update_device_info(cls, vm, device_conf):
        for x in vm.domain.get_device_elements('smartcard'):
            if vmxml.find_first(x, 'address', None) is None:
                continue

            address = vmxml.device_address(x)
            alias = vmxml.find_attr(x, 'alias', 'name')

            for dev in device_conf:
                if not hasattr(dev, 'address'):
                    dev.address = address
                    dev.alias = alias

            for dev in vm.conf['devices']:
                if dev['type'] == hwclass.SMARTCARD and \
                        not dev.get('address'):
                    dev['address'] = address
                    dev['alias'] = alias


class Sound(Base):
    __slots__ = ('address',)

    def getXML(self):
        """
        Create domxml for sound device
        """
        sound = self.createXmlElem('sound', None, ['address'])
        sound.setAttrs(model=self.device)
        return sound

    @classmethod
    def update_device_info(cls, vm, device_conf):
        for x in vm.domain.get_device_elements('sound'):
            alias = vmxml.find_attr(x, 'alias', 'name')
            # Get sound card address
            address = vmxml.device_address(x)

            # FIXME. We have an identification problem here.
            # Sound device has not unique identifier, except the alias
            # (but backend not aware to device's aliases). So, for now
            # we can only assign the address according to devices order.
            for sc in device_conf:
                if not hasattr(sc, 'address') or not hasattr(sc, 'alias'):
                    sc.alias = alias
                    sc.address = address
                    break
            # Update vm's conf with address
            for dev in vm.conf['devices']:
                if ((dev['type'] == hwclass.SOUND) and
                        (not dev.get('address') or not dev.get('alias'))):
                    dev['address'] = address
                    dev['alias'] = alias
                    break


class Redir(Base):
    __slots__ = ('address',)

    def getXML(self):
        """
        Create domxml for a redir device.
        <redirdev bus='usb' type='spicevmc'>
          <address type='usb' bus='0' port='1'/>
        </redirdev>
        """
        return self.createXmlElem('redirdev', self.device, ['bus', 'address'])


class Rng(Base):

    @staticmethod
    def matching_source(conf, source):
        return rngsources.get_device(conf['specParams']['source']) == source

    def uses_source(self, source):
        return rngsources.get_device(self.specParams['source']) == source

    def setup(self):
        if self.uses_source('/dev/hwrng'):
            supervdsm.getProxy().appropriateHwrngDevice(self.conf['vmId'])

    def teardown(self):
        if self.uses_source('/dev/hwrng'):
            supervdsm.getProxy().rmAppropriateHwrngDevice(self.conf['vmId'])

    def getXML(self):
        """
        <rng model='virtio'>
            <rate period="2000" bytes="1234"/>
            <backend model='random'>/dev/random</backend>
        </rng>
        """
        # TODO: we can simplify both schema and code getting rid
        # of either VmRngDeviceType or VmRngDeviceModel.
        # libvirt supports only one device type, 'virtio'.
        # To do so, we need
        # 1. to ensure complete test coverage
        # 2. cleanup attribute access and names:
        #    we use the 'model' attribute here, does it map
        #    to VmRngDeviceModel? Why we need VmRngDeviceType.
        rng = self.createXmlElem('rng', None, ['model'])

        # <rate... /> element
        if 'bytes' in self.specParams:
            rateAttrs = {'bytes': self.specParams['bytes']}
            if 'period' in self.specParams:
                rateAttrs['period'] = self.specParams['period']

            rng.appendChildWithArgs('rate', None, **rateAttrs)

        # <backend... /> element
        rng_dev = rngsources.get_device(self.specParams['source'])
        rng.appendChildWithArgs('backend', rng_dev, model='random')

        return rng

    @classmethod
    def update_device_info(cls, vm, device_conf):
        for rng in vm.domain.get_device_elements('rng'):
            address = vmxml.device_address(rng)
            alias = vmxml.find_attr(rng, 'alias', 'name')
            source = vmxml.text(vmxml.find_first(rng, 'backend'))

            for dev in device_conf:
                if dev.uses_source(source) and not hasattr(dev, 'alias'):
                    dev.address = address
                    dev.alias = alias
                    break

            for dev in vm.conf['devices']:
                if dev['type'] == hwclass.RNG and \
                   Rng.matching_source(dev, source) and \
                   'alias' not in dev:
                    dev['address'] = address
                    dev['alias'] = alias
                    break


class Tpm(Base):
    __slots__ = ()

    def getXML(self):
        """
        Add tpm section to domain xml

        <tpm model='tpm-tis'>
            <backend type='passthrough'>
                <device path='/dev/tpm0'>
            </backend>
        </tpm>
        """
        tpm = self.createXmlElem(self.device, None)
        tpm.setAttrs(model=self.specParams['model'])
        backend = tpm.appendChildWithArgs('backend',
                                          type=self.specParams['mode'])
        backend.appendChildWithArgs('device',
                                    path=self.specParams['path'])
        return tpm


class Video(Base):
    def getXML(self):
        """
        Create domxml for video device
        """
        video = self.createXmlElem('video', None, ['address'])
        sourceAttrs = {'vram': self.specParams.get('vram', '32768'),
                       'heads': self.specParams.get('heads', '1')}
        for attr in ('ram', 'vgamem',):
            if attr in self.specParams:
                sourceAttrs[attr] = self.specParams[attr]

        video.appendChildWithArgs('model', type=self.device, **sourceAttrs)
        return video

    @classmethod
    def update_device_info(cls, vm, device_conf):
        for x in vm.domain.get_device_elements('video'):
            alias = vmxml.find_attr(x, 'alias', 'name')
            # Get video card address
            address = vmxml.device_address(x)

            # FIXME. We have an identification problem here.
            # Video card device has not unique identifier, except the alias
            # (but backend not aware to device's aliases). So, for now
            # we can only assign the address according to devices order.
            for vc in device_conf:
                if not hasattr(vc, 'address') or not hasattr(vc, 'alias'):
                    vc.alias = alias
                    vc.address = address
                    break
            # Update vm's conf with address
            for dev in vm.conf['devices']:
                if ((dev['type'] == hwclass.VIDEO) and
                        (not dev.get('address') or not dev.get('alias'))):
                    dev['address'] = address
                    dev['alias'] = alias
                    break


class Watchdog(Base):
    __slots__ = ('address',)

    def __init__(self, *args, **kwargs):
        super(Watchdog, self).__init__(*args, **kwargs)

        if not hasattr(self, 'specParams'):
            self.specParams = {}

    @classmethod
    def update_device_info(cls, vm, device_conf):
        for x in vm.domain.get_device_elements('watchdog'):

            # PCI watchdog has "address" different from ISA watchdog
            if vmxml.find_first(x, 'address', None) is not None:
                address = vmxml.device_address(x)
                alias = vmxml.find_attr(x, 'alias', 'name')

                for wd in device_conf:
                    if not hasattr(wd, 'address') or not hasattr(wd, 'alias'):
                        wd.address = address
                        wd.alias = alias

                for dev in vm.conf['devices']:
                    if ((dev['type'] == hwclass.WATCHDOG) and
                            (not dev.get('address') or not dev.get('alias'))):
                        dev['address'] = address
                        dev['alias'] = alias

    def getXML(self):
        """
        Create domxml for a watchdog device.

        <watchdog model='i6300esb' action='reset'>
          <address type='pci' domain='0x0000' bus='0x00' slot='0x05'
           function='0x0'/>
        </watchdog>
        """
        m = self.createXmlElem(self.type, None, ['address'])
        m.setAttrs(model=self.specParams.get('model', 'i6300esb'),
                   action=self.specParams.get('action', 'none'))
        return m


class Memory(Base):
    __slots__ = ('address', 'size', 'node')

    def __init__(self, conf, log, **kwargs):
        super(Memory, self).__init__(conf, log, **kwargs)
        # we get size in mb and send in kb
        self.size = int(kwargs.get('size')) * 1024
        self.node = kwargs.get('node')

    @classmethod
    def update_device_info(cls, vm, device_conf):
        for x in vm.domain.get_device_elements('memory'):
            alias = vmxml.find_attr(x, 'alias', 'name')
            # Get device address
            address = vmxml.device_address(x)

            for mem in device_conf:
                if not hasattr(mem, 'address') or not hasattr(mem, 'alias'):
                    mem.alias = alias
                    mem.address = address
                    break
            # Update vm's conf with address
            for dev in vm.conf['devices']:
                if ((dev['type'] == hwclass.MEMORY) and
                        (not dev.get('address') or not dev.get('alias'))):
                    dev['address'] = address
                    dev['alias'] = alias
                    break
            vm.conf['memSize'] = vm.domain.get_memory_size()

    def getXML(self):
        """
        <memory model='dimm'>
            <target>
                <size unit='KiB'>524287</size>
                <node>1</node>
            </target>
        </memory>
        """

        mem = self.createXmlElem('memory', None)
        mem.setAttrs(model='dimm')
        target = self.createXmlElem('target', None)
        mem.appendChild(target)
        size = self.createXmlElem('size', None)
        size.setAttrs(unit='KiB')
        size.appendTextNode(str(self.size))
        target.appendChild(size)
        node = self.createXmlElem('node', None)
        node.appendTextNode(str(self.node))
        target.appendChild(node)

        return mem


class Lease(Base):
    """
    VM lease device.

    See https://libvirt.org/formatdomain.html#elementsLease
    """
    __slots__ = ("id", "sd_id", "path", "offset")

    @classmethod
    def update_device_info(cls, vm, device_conf):
        # TODO: update conf from libvirt info
        pass

    def __init__(self, conf, log, **kwargs):
        super(Lease, self).__init__(conf, log, **kwargs)
        """
        Initialize a lease element.

        :param uuid id: Lease id, e.g. volume id for a volume lease, or vm id
            for a vm lease
        :param uuid sd_id: Storage domain uuid where lease file is located
        :param str path: Path to lease file or block device
        :param int offset: Offset in lease file in bytes
        """
        # TODO: validate arguments

    def setup(self):
        # TODO: validate lease with storage, fail if lease is not mapped to
        # lease id.
        pass

    def getXML(self):
        """
        Return xml element.

        <lease>
            <key>12523e3d-ad22-410c-8977-d2a7bf458a65</key>
            <lockspace>c2a6d7c8-8d81-4e01-9ed4-7eb670713448</lockspace>
            <target offset="1048576"
                    path="/dev/c2a6d7c8-8d81-4e01-9ed4-7eb670713448/leases"/>
        </lease>

        :rtype: `vmxml.Element`
        """
        lease = vmxml.Element('lease')
        lease.appendChildWithArgs('key', text=self.id)
        lease.appendChildWithArgs('lockspace', text=self.sd_id)
        lease.appendChildWithArgs('target', path=self.path,
                                  offset=str(self.offset))
        return lease
