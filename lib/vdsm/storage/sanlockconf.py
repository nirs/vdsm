# Copyright 2020 Red Hat, Inc.
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
Read and write sanlock configuration file.

This is a minimal parser, reading sanlock configuration as a dict of strings,
and writing the dict back to the file.

This is a very minimal implementation:
- Comments in the original configuration are not preserved.
- There is no validation for option names or values
- All options are treated as strings.

Example usage:

    >>> conf = sanlockconf.load()
    >>> conf
    {'max_worker_threads': '50'}
    >>> conf['our_host_name'] = 'c59d39ca-620b-4aad-8b50-97833e366664'
    >>> sunlockconf.dump(conf)

For details on the file syntax and available options see:
https://pagure.io/sanlock/blob/master/f/src/main.c#_2714
"""

import os
import tempfile
import io

import selinux

from . import fileUtils

SANLOCK_CONF = "/etc/sanlock/sanlock.conf"


def load():
    """
    Read sanlock coniguration to dict of option: value strings.
    """
    try:
        with open(SANLOCK_CONF) as f:
            conf = {}
            for line in f:
                if line.startswith(("#", "\n", " ", "\t")):
                    continue
                if "=" not in line:
                    continue
                key, val = line.split("=", 1)
                if not key:
                    continue
                conf[key.rstrip()] = val.strip()
            return conf
    except FileNotFoundError:
        return {}


def dump(conf):
    """
    Backup current configuration and write new configuration.

    Arguemnts:
        conf (dict): Dict of option: value strings

    Returns:
        Path to backup file if the original confugration was backed up.
    """
    backup_path = fileUtils.backup_file(SANLOCK_CONF)

    buf = io.StringIO()
    buf.write("# Configuration for vdsm\n")
    for key, val in conf.items():
        buf.write("{} = {}\n".format(key, val))

    data = buf.getvalue().encode("utf-8")
    fileUtils.atomic_write(SANLOCK_CONF, data, relabel=True)

    return backup_path


def _atomic_write(filename, data, mode=0o644):
    with tempfile.NamedTemporaryFile(
            mode="wb",
            dir=os.path.dirname(filename),
            prefix=os.path.basename(filename) + ".tmp",
            delete=False) as tmp:
        try:
            tmp.write(data)
            tmp.flush()
            selinux.restorecon(tmp.name)
            os.chmod(tmp.name, mode)
            os.rename(tmp.name, filename)
        except:
            os.unlink(tmp.name)
            raise
