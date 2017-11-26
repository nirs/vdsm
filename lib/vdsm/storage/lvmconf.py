#
# Copyright 2017 Red Hat, Inc.
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
lvmconf - Acccess lvm configuration file

This module provdes the LVMConfig class, for reading and writing lvm
configuration. This class is a simple wrapper around augeas.Augeas, providing
easier to use, ConfigParser like interface for accessing options.


Reading configuration
---------------------

Use the get* methods::

    with lvmconf.LVMConfig(path) as conf:
        conf.getlist("devices", "filter")                   # ["a|.*|"]
        conf.getint("global", "use_lvmetad")                # 1
        conf.getstr("activation", "missing_stripe_filler")  # "error"

Unlike ConfigParser, reading integer options as strings does not work, so this
class does not provide untyped get() or set() method. Using the wrong type
returns None.


Modyfing configuration
----------------------

Use the set* methods::

    with lvmconf.LVMConfig(path) as conf:
        conf.setlist("devices", "section", ["a|^/dev/sda2$|", "r|.*|"])
        conf.setint("global", "use_lvmetad", 0)
        conf.setstr("activation", "missing_stripe_filler", "ignore")
        conf.save()

We use augeas backup option to save the previous file at file.augsave.

Anyone can read lvm configuration, but to modify it you must run as root.


TODO
----

- add "# Vdsm: description..." comment when modifying a value possible with
  tricky xpath voodoo.

- add blank line before new options seems that is not supported by augeas.

- insert options after at the default location, under the default comment.
  should be possible using tricky xpath and lot of work.

- indent new options properly - seems that it is not supported in augeas.

"""

from __future__ import absolute_import

import logging

from augeas import Augeas

log = logging.getLogger("storage.lvmconf")


class LVMConfig(object):

    def __init__(self, path="/etc/lvm/lvm.conf"):
        self.path = path

        # Augeas loads by default tons of unneeded lenses and configuration
        # files. On my test host, it fails to load, trying to read my 500 MiB
        # /etc/lvm/archive/.
        #
        # These are the standard LVM lens includes:
        # /augeas/load/LVM/incl[1] /etc/lvm/lvm.conf
        # /augeas/load/LVM/incl[2] /etc/lvm/backup/*
        # /augeas/load/LVM/incl[3] /etc/lvm/archive/*.vg
        #
        # We need only the first entry to work with lvm.conf. Using customized
        # load setup, as explained in
        # https://github.com/hercules-team/augeas/wiki/Loading-specific-files
        #
        # Removing the archive and backup entries, we can load augeas in 0.7
        # seconds on my test vm. Removing all other lenses shorten the time to
        # 0.04 seconds.

        log.debug("Loading lvm configuration from %r", path)
        self.aug = Augeas(flags=Augeas.NO_MODL_AUTOLOAD | Augeas.SAVE_BACKUP)
        self.aug.add_transform("lvm.lns", [path])
        self.aug.load()

    # Context manager interface

    def __enter__(self):
        return self

    def __exit__(self, t, v, tb):
        try:
            self.close()
        except Exception as e:
            # Caller succeeded, raise the close error.
            if t is None:
                raise
            # Caller has failed, do not hide the original error.
            log.exception("Error closing %s: %s" % (self, e))

    # Accessing list options

    def getlist(self, section, option):
        pat = "/files%s/%s/dict/%s/list/*/str" % (self.path, section, option)
        matches = self.aug.match(pat)
        if not matches:
            return None  # Cannot store/read empty list
        return [self.aug.get(m) for m in matches]

    def setlist(self, section, option, value):
        log.debug("Setting %s/%s to %s", section, option, value)
        opt_path = "/files%s/%s/dict/%s" % (self.path, section, option)
        self.aug.remove(opt_path)
        val_path = opt_path + "/list/%d/str"
        for i, item in enumerate(value, 1):
            self.aug.set(val_path % i, item)

    # Accessing int options

    def getint(self, section, option):
        path = "/files%s/%s/dict/%s/int" % (self.path, section, option)
        val = self.aug.get(path)
        return int(val) if val is not None else None

    def setint(self, section, option, value):
        log.debug("Setting %s/%s to %s", section, option, value)
        path = "/files%s/%s/dict/%s/int" % (self.path, section, option)
        self.aug.set(path, str(value))

    # Accessing string options

    def getstr(self, section, option):
        path = "/files%s/%s/dict/%s/str" % (self.path, section, option)
        return self.aug.get(path)

    def setstr(self, section, option, value):
        log.debug("Setting %s/%s to %s", section, option, value)
        path = "/files%s/%s/dict/%s/str" % (self.path, section, option)
        self.aug.set(path, value)

    # Removing options

    def remove(self, section, option):
        log.debug("Removing %s/%s", section, option)
        path = "/files%s/%s/dict/%s" % (self.path, section, option)
        self.aug.remove(path)

    # File operations

    def save(self):
        import pprint
        log.info("Saving new lvm configuration to %r. Previous configuration "
                 "saved to %r",
                 self.path, self.path + ".augsave")
        self.aug.save()

    def close(self):
        log.debug("Closing lvm configuration %s", self.path)
        self.aug.close()
