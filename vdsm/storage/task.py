#
# Copyright 2009-2016 Red Hat, Inc.
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

'''
    Tasks: tasks object model some sort of VDSM task (storage task).
    A task object may be standalone (unmanaged task), but then it is limited
    and cannot be automatically persisted or run (asynchronous jobs).
    A managed task is managed by a TaskManager. Under the task manager a task
    may persist itself, run asynchronous jobs, run recovery procedures, etc.
    A task is successful if it finished its operations during the
    prepare state (i.e. it did not schedule any async jobs), or all its
    scheduled jobs complete without error.
    The task result is the prepare state result if no async jobs are scheduled,
    or the result of the last job run.

    Jobs: jobs object model an asynchronous job that is run in a context of a
    thread belonging to a thread pool (and managed by the task manager).
    Currently a task may schedule any number of jobs that run sequentially by
    the same worker thread.

    Recovery: recovery objects model a recovery operation required to restore
    the system to a known coherent state. A task may register several recovery
    objects that are kept in a stack (the last recovery registered is run
    first - lifo).
    Tasks that have "auto" recovery policy run the recovery procedure
    ("rollback"),
    in any case of task failure/abort, and immediately after the task is
    recovered (loaded) from its persisted store.
'''

import os
import logging
import threading
import types
from contextlib import contextmanager
from functools import wraps

from vdsm.storage import exception as se
from vdsm.storage.threadlocal import vars

import uuid
import misc
import resourceManager
from weakref import proxy
from vdsm import concurrent
from vdsm.config import config
from vdsm.logUtils import SimpleLogAdapter
import outOfProcess as oop


getProcPool = oop.getGlobalProcPool

KEY_SEPARATOR = "="
KEY_SEPARATOR_ENCODED = "_eq_"
TASK_EXT = ".task"
JOB_EXT = ".job"
RESOURCE_EXT = ".resource"
RECOVER_EXT = ".recover"
RESULT_EXT = ".result"
BACKUP_EXT = ".backup"
TEMP_EXT = ".temp"
NUM_SEP = "."
FIELD_SEP = ","
RESOURCE_SEP = "!"
TASK_METADATA_VERSION = 1

ROLLBACK_SENTINEL = "rollback sentinel"


def _eq_encode(s):
    if KEY_SEPARATOR_ENCODED in s:
        raise ValueError("%s includes %s" % (s, KEY_SEPARATOR_ENCODED))
    return s.replace(KEY_SEPARATOR, KEY_SEPARATOR_ENCODED)


def _eq_decode(s):
    return s.replace(KEY_SEPARATOR_ENCODED, KEY_SEPARATOR)


def threadlocal_task(m):
    """
    Decorator that set the task object in thread local storage task attribute
    while the decorated method is running.
    """
    @wraps(m)
    def wrapper(self, *a, **kw):
        vars.task = self
        try:
            return m(self, *a, **kw)
        finally:
            vars.task = None
    return wrapper


class State:
    unknown = "unknown"
    init = "init"
    preparing = "preparing"
    blocked = "blocked"
    acquiring = "acquiring"
    queued = "queued"
    running = "running"
    finished = "finished"
    aborting = "aborting"
    waitrecover = "waitrecover"
    recovering = "recovering"
    racquiring = "racquiring"
    raborting = "raborting"
    recovered = "recovered"
    failed = "failed"

    # For backward compatibility, inner states can be translated to
    # previously supported
    # format of state and result
    DEPRECATED_STATE = {
        unknown: "unknown",
        init: "init",
        preparing: "running",
        blocked: "running",
        acquiring: "running",
        queued: "running",
        running: "running",
        finished: "finished",
        aborting: "aborting",
        waitrecover: "cleaning",
        recovering: "cleaning",
        racquiring: "cleaning",
        raborting: "aborting",
        recovered: "finished",
        failed: "finished"
    }

    DEPRECATED_RESULT = {
        unknown: "",
        init: "",
        preparing: "",
        blocked: "",
        acquiring: "",
        queued: "",
        running: "",
        finished: "success",
        aborting: "",
        waitrecover: "",
        recovering: "",
        racquiring: "",
        raborting: "",
        recovered: "cleanSuccess",
        failed: "cleanFailure"
    }

    # valid state transitions: newstate <- [from states]
    _moveto = {
        unknown: [],
        init: [],
        preparing: [init, blocked],
        blocked: [preparing],
        acquiring: [preparing, acquiring],
        queued: [acquiring, running],
        running: [queued],
        finished: [running, preparing],
        aborting: [preparing, blocked, acquiring, queued, running],
        waitrecover: [aborting],
        racquiring: [aborting, finished, racquiring, waitrecover],
        recovering: [racquiring],
        raborting: [racquiring, recovering, waitrecover],
        recovered: [recovering],
        failed: [recovering, aborting, raborting],
    }
    _done = [finished, recovered, failed]
    _recovering = [racquiring, recovering]

    def __init__(self, state=None):
        try:
            if not state:
                self.state = self.unknown
            else:
                self.state = getattr(self, state)
        except:
            self.state = self.unknown

    def value(self):
        return self.state

    def isDone(self):
        return self.state in self._done

    def isRecovering(self):
        return self.state in self._recovering

    def canAbort(self):
        return self.state in self._moveto[self.aborting]

    def canAbortRecovery(self):
        return self.state in self._moveto[self.raborting]

    def __str__(self):
        return self.state

    def moveto(self, state, force=False):
        if state not in self._moveto:
            raise ValueError("not a valid target state: %s" % state)
        if not force and self.state not in self._moveto[state]:
            raise se.TaskStateTransitionError("from %s to %s" %
                                              (self.state, state))
        self.state = state

    def __eq__(self, state):
        return self.state == state

    def __ne__(self, state):
        return self.state != state


class EnumType(object):
    def __init__(self, enum):
        if not getattr(self, enum, None):
            raise ValueError("%s not a valid type for %s" % (enum, repr(self)))
        self.value = enum

    def __str__(self):
        return str(self.value)

    def __eq__(self, x):
        if type(x) == str:
            return self.value == x
        if isinstance(x, self):
            return x.value == self.value
        return False

    def __ne__(self, x):
        return not self.__eq__(x)


class TaskPersistType(EnumType):
    none = "none"
    manual = "manual"
    auto = "auto"


class TaskCleanType(EnumType):
    none = "none"
    manual = "manual"
    auto = "auto"


class TaskRecoveryType(EnumType):
    none = "none"
    manual = "manual"
    auto = "auto"


class ParamList:

    def __init__(self, params="", sep=FIELD_SEP):
        self.sep = sep
        if not params:
            self.params = ""

        if type(params) == list:
            for i in params:
                if not isinstance(i, types.StringTypes):
                    raise ValueError("ParamsList: param item %s not a string"
                                     " (%s)" % (i, type(i)))
                if sep in i:
                    raise ValueError("ParamsList: sep %s in %s" % (sep, i))
            self.params = params
        elif isinstance(params, types.StringTypes):
            self.params = [s.strip() for s in params.split(sep)]
        else:
            raise ValueError("ParamList: params type not supported (%s)" %
                             type(params))

    def getList(self):
        return self.params[:]

    def __str__(self):
        s = ""
        for i in self.params:
            s += unicode(i) + self.sep
        # remove last sep
        if s:
            s = s[:-1]
        return s


class Job:
    fields = {
        "name": unicode,
        "runcmd": unicode,
    }

    def __init__(self, name, cmd, *argslist, **argsdict):
        self.name = name
        self.cmd = cmd       # function pointer to run
        self.argslist = argslist
        self.argsdict = argsdict
        self.runcmd = "%r (args: %s kwargs: %s)" % (cmd, argslist, argsdict)
        self.callback = None    # callback to call before running the job
        self.task = None

    def setCallback(self, callback):
        if not callable(callback):
            raise ValueError("Task.Job: callback %s is not callable" %
                             repr(callback))
        self.callback = callback

    def setOwnerTask(self, task):
        self.task = proxy(task)

    def run(self):
        if not self.task:
            raise se.InvalidJob("Job %s: no parent task" % self)
        self.task.log.debug("Job.run: running %s callback %r",
                            self, self.callback)
        if self.callback:
            self.callback(self)
        return self.cmd(*self.argslist, **self.argsdict)

    def __str__(self):
        return "%s: %s" % (self.name, self.runcmd)


class Recovery:
    '''
    A recovery object is used to register a recovery function to
    the recovery process.
    The recovery functions are kept in a stack so they are carried out
    in FILO order.
    The recovery function itself must be a static/class binded method of
    a visible class.
    The recovery function must accept a recovery object as the first
    parameter - fn(recovery, ...)
    All other parameters if any must be strings.
    '''
    fields = {
        "name": unicode,
        "moduleName": unicode,
        "object": unicode,
        "function": unicode,
        "params": ParamList,
    }

    def __init__(self, name, modname, objname, fnname, argslist):
        self.name = name
        self.validateName(modname)
        self.validateName(objname)
        self.validateName(modname)
        self.validateName(fnname)
        self.object = objname
        self.moduleName = modname
        self.function = fnname
        self.params = ParamList(argslist)
        self.callback = None
        self.task = None

    def validateName(self, name):
        vname = name.replace("_", "")
        if not vname.isalnum():
            raise TypeError("Parameter %s must be a plain str" % name)

    def setCallback(self, callback):
        if not callable(callback):
            raise ValueError("Task.Recovery: callback %s is not callable" %
                             repr(callback))
        self.task.log.debug("Recovery.run: running %s callback %r",
                            self, self.callback)
        self.callback = callback

    def setOwnerTask(self, task):
        self.task = proxy(task)

    def run(self):
        if not self.task:
            raise se.InvalidRecovery("Recovery - %s: no parent task" % self)
        self.validateName(self.object)
        self.validateName(self.function)
        if self.callback:
            self.callback(self)
        # instantiate an object of class "self.object" (bad name)
        module = __import__('storage.' + self.moduleName,
                            locals(), globals(),
                            [self.moduleName])
        classObj = getattr(module, self.object)
        function = getattr(classObj, self.function)
        argslist = self.params.getList()
        return function(self.task, *argslist)

    def __str__(self):
        return "%s: %s->%s(%s)" % (self.name,
                                   self.object,
                                   self.function,
                                   self.params)


class TaskResult(object):
    fields = {
        "code": int,
        "message": unicode,
        "result": unicode,
    }

    def __init__(self, code=0, message="", result=""):
        self.code = code
        self.message = message
        self.result = result

    def toDict(self):
        return dict(message=self.message,
                    code=str(self.code),
                    result=self.result)

    def __str__(self):
        return "Task result: %s - %s: %s" % (self.code,
                                             self.message,
                                             self.result)


class TaskPriority(EnumType):
    low = "low"
    medium = "medium"
    high = "high"


class Task:
    # External Task info
    fields = {
        # field_name: type
        "id": str,
        "name": unicode,
        "tag": unicode,
        "store": unicode,
        "recoveryPolicy": TaskRecoveryType,
        "persistPolicy": TaskPersistType,
        "cleanPolicy": TaskCleanType,
        "priority": TaskPriority,
        "state": State,
        "njobs": int,
        "nrecoveries": int,
        "metadataVersion": int
    }

    log = logging.getLogger('Storage.TaskManager.Task')

    def __init__(self, id, name="", tag="",
                 recovery=TaskRecoveryType.none,
                 priority=TaskPriority.low):
        """
        id - Unique ID
        name - human readable name
        persist - persistency type: auto-clean/manual-clean/not-persistent
        """

        if not id:
            id = str(uuid.uuid4())
        self.metadataVersion = TASK_METADATA_VERSION
        self.validateID(id)
        self.lock = threading.Lock()
        self.callbackLock = threading.Lock()
        self.id = str(id)
        self.name = name
        self.tag = tag
        self.priority = priority
        self.recoveryPolicy = recovery
        self.persistPolicy = TaskPersistType.none
        self.cleanPolicy = TaskCleanType.auto
        self.store = None
        self.defaultException = None

        self.state = State(State.init)
        self.result = TaskResult(0, "Task is initializing", "")

        self.resOwner = resourceManager.Owner(proxy(self), raiseonfailure=True)
        self.error = se.TaskAborted("Unknown error encountered")

        self.mng = None
        self._abort_lock = threading.Lock()
        self._abort_callbacks = set()
        self._aborting = False
        self._forceAbort = False
        self.ref = 0

        self.recoveries = []
        self.jobs = []
        self.nrecoveries = 0    # just utility count - used by save/load
        self.njobs = 0          # just utility count - used by save/load

        self.log = SimpleLogAdapter(self.log, {"Task": self.id})

    def __del__(self):
        def finalize(log, owner, taskDir):
            log.warn("Task was autocleaned")
            owner.releaseAll()
            if taskDir is not None:
                getProcPool().fileUtils.cleanupdir(taskDir)

        if not self.state.isDone():
            taskDir = None
            if (self.cleanPolicy == TaskCleanType.auto and
                    self.store is not None):
                taskDir = os.path.join(self.store, self.id)
            concurrent.thread(finalize,
                              args=(self.log, self.resOwner, taskDir)).start()

    def _done(self):
        self.resOwner.releaseAll()
        if self.cleanPolicy == TaskCleanType.auto:
            self.clean()

    def __state_preparing(self, fromState):
        pass

    def __state_blocked(self, fromState):
        pass

    def __state_acquiring(self, fromState):
        if self.resOwner.requestsGranted():
            self._updateState(State.queued)

    def __state_queued(self, fromState):
        try:
            self.mng.queue(self)
        except Exception as e:
            self._setError(e)
            self.stop()

    def __state_running(self, fromState):
        self._runJobs()

    def __state_finished(self, fromState):
        self._done()

    def __state_aborting(self, fromState):
        if self.ref > 1:
            return
        self.log.debug("_aborting: recover policy %s", self.recoveryPolicy)
        if self.recoveryPolicy == TaskRecoveryType.auto:
            self._updateState(State.racquiring)
        elif self.recoveryPolicy == TaskRecoveryType.none:
            self._updateState(State.failed)
        else:
            self._updateState(State.waitrecover)

    def __state_waitrecover(self, fromState):
        pass

    def __state_racquiring(self, fromState):
        if self.resOwner.requestsGranted():
            self._updateState(State.recovering)

    def __state_recovering(self, fromState):
        self._recover()

    def __state_raborting(self, fromState):
        if self.ref == 1:
            self._updateState(State.failed)
        else:
            self.log.warn("State was change to 'raborting' "
                          "when ref was not 1.")

    def __state_recovered(self, fromState):
        self._done()

    def __state_failed(self, fromState):
        self._done()

    def __state_cleaning(self, fromState):
        pass

    def _updateState(self, state, force=False):
        fromState = self.state
        requestedState = state
        if self._aborting:
            if self.state.canAbort():
                state = State.aborting
            elif self.state.canAbortRecovery() and state != State.recovered:
                state = State.raborting
        self._aborting = False
        if requestedState == state:
            self.log.debug("moving from state %s -> state %s",
                           fromState, state)
        else:
            self.log.debug("moving from state %s -> state %s instead of %s",
                           fromState, state, requestedState)

        self.state.moveto(state, force)
        if self.persistPolicy == TaskPersistType.auto:
            try:
                self.persist()
            except Exception:
                self.log.warning("Task._updateState: failed persisting task"
                                 " %s", self.id, exc_info=True)

        fn = getattr(self, "_Task__state_%s" % state)
        fn(fromState)

    def _updateResult(self, code, message, result):
        self.result.result = result
        self.result.code = code
        self.result.message = message

    @classmethod
    def validateID(cls, taskID):
        if not taskID or "." in taskID:
            raise se.InvalidParameterException("taskID", taskID)

    @classmethod
    def _loadMetaFile(cls, filename, obj, fields):
        try:
            for line in getProcPool().readLines(filename):
                # process current line
                line = line.encode('utf8')
                if line.find(KEY_SEPARATOR) < 0:
                    continue
                parts = line.split(KEY_SEPARATOR)
                if len(parts) != 2:
                    cls.log.warning("Task._loadMetaFile: %s - ignoring line"
                                    " '%s'", filename, line)
                    continue

                field = _eq_decode(parts[0].strip())
                value = _eq_decode(parts[1].strip())
                if field not in fields:
                    cls.log.warning("Task._loadMetaFile: %s - ignoring field"
                                    " %s in line '%s'", filename, field, line)
                    continue

                ftype = fields[field]
                setattr(obj, field, ftype(value))
        except Exception:
            cls.log.error("Unexpected error", exc_info=True)
            raise se.TaskMetaDataLoadError(filename)

    @classmethod
    def _dump(cls, obj, fields):
        lines = []
        for field in fields:
            try:
                value = unicode(getattr(obj, field))
            except AttributeError:
                cls.log.warning("object %s field %s not found" %
                                (obj, field), exc_info=True)
            else:
                try:
                    field = _eq_encode(field)
                    value = _eq_encode(value)
                except ValueError as e:
                    cls.log.warning("Object %s: Cannot encode field %s or "
                                    "value %s. Skipping field. %s",
                                    obj, field, value, e)
                else:
                    lines.append("%s %s %s" % (field, KEY_SEPARATOR, value))
        return lines

    @classmethod
    def _saveMetaFile(cls, filename, obj, fields):
        try:
            getProcPool().writeLines(filename,
                                     [l.encode('utf8') + "\n"
                                      for l in cls._dump(obj, fields)])
        except Exception:
            cls.log.error("Unexpected error", exc_info=True)
            raise se.TaskMetaDataSaveError(filename)

    def _loadTaskMetaFile(self, taskDir):
        taskFile = os.path.join(taskDir, self.id + TASK_EXT)
        self._loadMetaFile(taskFile, self, Task.fields)

    def _saveTaskMetaFile(self, taskDir):
        taskFile = os.path.join(taskDir, self.id + TASK_EXT)
        self._saveMetaFile(taskFile, self, Task.fields)

    def _loadJobMetaFile(self, taskDir, n):
        taskFile = os.path.join(taskDir, self.id + JOB_EXT + NUM_SEP + str(n))
        self._loadMetaFile(taskFile, self.jobs[n], Job.fields)

    def _saveJobMetaFile(self, taskDir, n):
        taskFile = os.path.join(taskDir, self.id + JOB_EXT + NUM_SEP + str(n))
        self._saveMetaFile(taskFile, self.jobs[n], Job.fields)

    def _loadRecoveryMetaFile(self, taskDir, n):
        taskFile = os.path.join(taskDir,
                                self.id + RECOVER_EXT + NUM_SEP + str(n))
        self._loadMetaFile(taskFile, self.recoveries[n], Recovery.fields)

    def _saveRecoveryMetaFile(self, taskDir, n):
        taskFile = os.path.join(taskDir,
                                self.id + RECOVER_EXT + NUM_SEP + str(n))
        self._saveMetaFile(taskFile, self.recoveries[n], Recovery.fields)

    def _loadTaskResultMetaFile(self, taskDir):
        taskFile = os.path.join(taskDir, self.id + RESULT_EXT)
        self._loadMetaFile(taskFile, self.result, TaskResult.fields)

    def _saveTaskResultMetaFile(self, taskDir):
        taskFile = os.path.join(taskDir, self.id + RESULT_EXT)
        self._saveMetaFile(taskFile, self.result, TaskResult.fields)

    def _getResourcesKeyList(self, taskDir):
        keys = []
        for path in getProcPool().glob.glob(os.path.join(taskDir,
                                                         "*" + RESOURCE_EXT)):
            filename = os.path.basename(path)
            keys.append(filename[:filename.rfind(RESOURCE_EXT)])
        return keys

    def _load(self, storPath, ext=""):
        self.log.debug("%s: load from %s, ext '%s'", self, storPath, ext)
        if self.state != State.init:
            raise se.TaskMetaDataLoadError("task %s - can't load self: "
                                           "not in init state" % self)
        taskDir = os.path.join(storPath, str(self.id) + str(ext))
        if not getProcPool().os.path.exists(taskDir):
            raise se.TaskDirError("load: no such task dir '%s'" % taskDir)
        oldid = self.id
        self._loadTaskMetaFile(taskDir)
        if self.id != oldid:
            raise se.TaskMetaDataLoadError("task %s: loaded file do not match"
                                           " id (%s != %s)" %
                                           (self, self.id, oldid))
        if self.state == State.finished:
            self._loadTaskResultMetaFile(taskDir)
        for jn in range(self.njobs):
            self.jobs.append(Job("load", None))
            self._loadJobMetaFile(taskDir, jn)
            self.jobs[jn].setOwnerTask(self)
        for rn in range(self.nrecoveries):
            self.recoveries.append(Recovery("load", "load",
                                            "load", "load", ""))
            self._loadRecoveryMetaFile(taskDir, rn)
            self.recoveries[rn].setOwnerTask(self)

    def _save(self, storPath):
        origTaskDir = os.path.join(storPath, self.id)
        if not getProcPool().os.path.exists(origTaskDir):
            raise se.TaskDirError("_save: no such task dir '%s'" % origTaskDir)
        taskDir = os.path.join(storPath, self.id + TEMP_EXT)
        self.log.debug("_save: orig %s temp %s", origTaskDir, taskDir)
        if getProcPool().os.path.exists(taskDir):
            getProcPool().fileUtils.cleanupdir(taskDir)
        getProcPool().os.mkdir(taskDir)
        try:
            self.njobs = len(self.jobs)
            self.nrecoveries = len(self.recoveries)
            self._saveTaskMetaFile(taskDir)
            if self.state == State.finished:
                self._saveTaskResultMetaFile(taskDir)
            for jn in range(self.njobs):
                self._saveJobMetaFile(taskDir, jn)
            for rn in range(self.nrecoveries):
                self._saveRecoveryMetaFile(taskDir, rn)
        except Exception as e:
            self.log.error("Unexpected error", exc_info=True)
            try:
                getProcPool().fileUtils.cleanupdir(taskDir)
            except:
                self.log.warning("can't remove temp taskdir %s" % taskDir)
            raise se.TaskPersistError("%s persist failed: %s" % (self, e))
        # Make sure backup dir doesn't exist
        getProcPool().fileUtils.cleanupdir(origTaskDir + BACKUP_EXT)
        getProcPool().os.rename(origTaskDir, origTaskDir + BACKUP_EXT)
        getProcPool().os.rename(taskDir, origTaskDir)
        getProcPool().fileUtils.cleanupdir(origTaskDir + BACKUP_EXT)
        getProcPool().fileUtils.fsyncPath(origTaskDir)

    def _clean(self, storPath):
        taskDir = os.path.join(storPath, self.id)
        getProcPool().fileUtils.cleanupdir(taskDir)

    def _recoverDone(self):
        # protect agains races with stop/abort
        self.log.debug("Recover Done: state %s", self.state)
        while True:
            try:
                if self.state == State.recovering:
                    self._updateState(State.recovered)
                elif self.state == State.raborting:
                    self._updateState(State.failed)
                return
            except se.TaskStateTransitionError:
                self.log.error("Unexpected error", exc_info=True)

    def _recover(self):
        self.log.debug("_recover")
        if not self.state == State.recovering:
            raise se.TaskStateError("%s: _recover in state %s" %
                                    (self, self.state))
        try:
            while self.state == State.recovering:
                rec = self.popRecovery()
                self.log.debug("running recovery %s", rec)
                if not rec:
                    break
                self._run(rec.run)
        except Exception as e:
            self.log.warning("task %s: recovery failed: %s",
                             self, e, exc_info=True)
            # protect agains races with stop/abort
            try:
                if self.state == State.recovering:
                    self._updateState(State.raborting)
            except se.TaskStateTransitionError:
                pass
        self._recoverDone()

    def resourceAcquired(self, namespace, resource, locktype):
        # Callback from resourceManager.Owner. May be called by another thread.
        self._incref()
        try:
            self.callbackLock.acquire()
            try:
                self.log.debug("_resourcesAcquired: %s.%s (%s)",
                               namespace, resource, locktype)
                if self.state == State.preparing:
                    return
                if self.state == State.acquiring:
                    self._updateState(State.acquiring)
                elif self.state == State.racquiring:
                    self._updateState(State.racquiring)
                elif self.state == State.blocked:
                    self._updateState(State.preparing)
                elif (self.state == State.aborting or
                      self.state == State.raborting):
                    self.log.debug("resource %s.%s acquired while in state %s",
                                   namespace, resource, self.state)
                else:
                    raise se.TaskStateError("acquire is not allowed in state"
                                            " %s" % self.state)
            finally:
                self.callbackLock.release()
        finally:
            self._decref()

    def resourceRegistered(self, namespace, resource, locktype):
        self._incref()
        try:
            self.callbackLock.acquire()
            try:
                # Callback from resourceManager.Owner. May be called
                # by another thread.
                self.log.debug("_resourcesAcquired: %s.%s (%s)",
                               namespace, resource, locktype)
                # Protect against races with stop/abort
                if self.state == State.preparing:
                    self._updateState(State.blocked)
            finally:
                self.callbackLock.release()
        finally:
            self._decref()

    def _setError(self, e=se.TaskAborted("Unknown error encountered")):
        self.log.error("Unexpected error", exc_info=True)
        self.error = e

    def _run(self, fn, *args, **kargs):
        code = 100
        message = "Unknown Error"
        try:
            return fn(*args, **kargs)
        except se.StorageException as e:
            code = e.code
            message = e.message
            self._setError(e)
        except Exception as e:
            message = unicode(e)
            self._setError(e)
        except:
            self._setError()

        self.log.debug("Task._run: %s %s %s failed - stopping task",
                       self, args, kargs)
        self.stop()
        raise se.TaskAborted(message, code)

    def _runJobs(self):
        result = ""
        code = 100
        message = "Unknown Error"
        i = 0
        j = None
        try:
            if self.aborting():
                raise se.TaskAborted("shutting down")
            if not self.state == State.running:
                raise se.TaskStateError("%s: can't run Jobs in state %s" %
                                        (self, self.state))
            # for now: result is the last job result, jobs are run sequentially
            for j in self.jobs:
                if self.aborting():
                    raise se.TaskAborted("shutting down")
                self.log.debug("Task.run: running job %s: %s" % (i, j))
                self._updateResult(
                    0, 'running job {0} of {0}'.format(i + 1, len(self.jobs)),
                    '')
                result = self._run(j.run)
                if result is None:
                    result = ""
                i += 1
            j = None
            self._updateResult(0, "%s jobs completed successfully" % i, result)
            self._updateState(State.finished)
            self.log.debug('Task.run: exit - success: result %s' % result)
            return result
        except se.TaskAborted as e:
            self.log.debug("aborting: %s", e)
            message = e.value
            code = e.abortedcode
            if not self.aborting():
                self.log.error("Aborted exception but not in aborting state")
                raise
        self._updateResult(code, message, "")

    def _doAbort(self, force=False):
        self.log.debug("Task._doAbort: force %s" % force)
        self.lock.acquire()
        # Am I really the last?
        if self.ref != 0:
            self.lock.release()
            return
        self.ref += 1
        self.lock.release()
        try:
            try:
                if (not self.state.canAbort() and
                        (force and not self.state.canAbortRecovery())):
                    self.log.warning("Task._doAbort %s: ignoring - "
                                     "at state %s", self, self.state)
                    return
                self.resOwner.cancelAll()
                if self.state.canAbort():
                    self._updateState(State.aborting)
                else:
                    self._updateState(State.raborting)
            except se.TaskAborted:
                self._updateState(State.failed)
        finally:
            self.lock.acquire()
            self.ref -= 1
            self.lock.release()
            # If something horrible went wrong. Just fail the task.
            if not self.state.isDone():
                self.log.warn("Task exited in non terminal state. "
                              "Setting tasks as failed.")
                self._updateState(State.failed)

    def _doRecover(self):
        self.lock.acquire()
        # Am I really the last?
        if self.ref != 0:
            self.lock.release()
            raise se.TaskHasRefs(unicode(self))
        self.ref += 1
        self.lock.release()
        try:
            self._updateState(State.racquiring)
        finally:
            self.lock.acquire()
            self.ref -= 1
            self.lock.release()

    def _incref(self, force=False):
        self.lock.acquire()
        try:
            if self.aborting() and (self._forceAbort or not force):
                raise se.TaskAborted(unicode(self))

            self.ref += 1
            ref = self.ref
            return ref
        finally:
            self.lock.release()

    def _decref(self, force=False):
        self.lock.acquire()
        self.ref -= 1
        ref = self.ref
        self.lock.release()

        self.log.debug("ref %d aborting %s", ref, self.aborting())
        if ref == 0 and self.aborting():
            self._doAbort(force)
        return ref

    ##########################################################################
    # Public Interface                                                       #
    ##########################################################################

    def setDefaultException(self, exceptionObj):
        # defaultException must have response method
        if exceptionObj and not hasattr(exceptionObj, "response"):
            raise se.InvalidDefaultExceptionException(unicode(exceptionObj))
        self.defaultException = exceptionObj

    def setTag(self, tag):
        if KEY_SEPARATOR in tag:
            raise ValueError("tag cannot include %s character" % KEY_SEPARATOR)
        self.tag = unicode(tag)

    def isDone(self):
        return self.state.isDone()

    def addJob(self, job):
        """
        Add async job to the task. Assumes all resources are acquired
        or registered.
        """
        if not self.mng:
            raise se.UnmanagedTask(unicode(self))
        if not isinstance(job, Job):
            raise TypeError("Job param %s(%s) must be Job object" %
                            (repr(job), type(job)))
        if self.state != State.preparing:
            raise Exception("Task.addJob: can't add job in non preparing state"
                            " (%s)" % self.state)
        if not job.name:
            raise ValueError("Task.addJob: name is required")
        name = job.name
        for j in self.jobs:
            if name == j.name:
                raise ValueError("addJob: name '%s' must be unique" % (name))
        job.setOwnerTask(self)
        self.jobs.append(job)
        self.njobs = len(self.jobs)

    def clean(self):
        if not self.store:
            return
        if not self.isDone():
            raise se.TaskStateError("can't clean in state %s" % self.state)
        self._clean(self.store)

    def pushRecovery(self, recovery):
        """
        Add recovery "job" to the task. Recoveries are committed in FILO order.
        Assumes that all required resources are acquired or registered.
        """
        if not isinstance(recovery, Recovery):
            raise TypeError("recovery param %s(%s) must be Recovery object" %
                            (repr(recovery), type(recovery)))
        if not recovery.name:
            raise ValueError("pushRecovery: name is required")
        name = recovery.name
        for r in self.recoveries:
            if name == r.name:
                raise ValueError("pushRecovery: name '%s' must be unique" %
                                 (name))
        recovery.setOwnerTask(self)
        self.recoveries.append(recovery)
        self.persist()

    def replaceRecoveries(self, recovery):
        if not isinstance(recovery, Recovery):
            raise TypeError("recovery param %s(%s) must be Recovery object" %
                            (repr(recovery), type(recovery)))
        if not recovery.name:
            raise ValueError("replaceRecoveries: name is required")
        recovery.setOwnerTask(self)
        rec = Recovery('stubName', 'stubMod', 'stubObj', 'stubFunc', [])
        while (rec and (rec.name != ROLLBACK_SENTINEL)):
            rec = self.popRecovery()
        self.recoveries.append(recovery)
        self.persist()

    def popRecovery(self):
        if self.recoveries:
            return self.recoveries.pop()

    def clearRecoveries(self):
        self.recoveries = []
        self.persist()

    def setManager(self, manager):
        # If need be, refactor out to "validateManager" method
        if not hasattr(manager, "queue"):
            raise se.InvalidTaskMng(unicode(manager))
        self.mng = manager

    def setCleanPolicy(self, clean):
        self.cleanPolicy = TaskCleanType(clean)

    def setPersistence(self, store,
                       persistPolicy=TaskPersistType.auto,
                       cleanPolicy=TaskCleanType.auto):
        self.persistPolicy = TaskPersistType(persistPolicy)
        self.store = store
        self.setCleanPolicy(cleanPolicy)
        if self.persistPolicy != TaskPersistType.none and not self.store:
            raise se.TaskPersistError("no store defined")
        taskDir = os.path.join(self.store, self.id)
        try:
            getProcPool().fileUtils.createdir(taskDir)
        except Exception as e:
            self.log.error("Unexpected error", exc_info=True)
            raise se.TaskPersistError("%s: cannot access/create taskdir"
                                      " %s: %s" % (self, taskDir, e))
        if (self.persistPolicy == TaskPersistType.auto and
                self.state != State.init):
            self.persist()

    def setRecoveryPolicy(self, clean):
        self.recoveryPolicy = TaskRecoveryType(clean)

    def rollback(self):
        self.log.debug('(rollback): enter')
        if self.recoveryPolicy == TaskRecoveryType.none:
            self.log.debug("rollback is skipped")
            return
        if not self.isDone():
            raise se.TaskNotFinished("can't rollback in state %s" % self.state)
        self._doRecover()
        self.log.debug('(rollback): exit')

    def persist(self):
        if self.persistPolicy == TaskPersistType.none:
            return
        if not self.store:
            raise se.TaskPersistError("no store defined")
        if self.state == State.init:
            raise se.TaskStateError("can't persist in state %s" % self.state)
        self._save(self.store)

    @classmethod
    def loadTask(cls, store, taskid):
        t = Task(taskid)
        if getProcPool().os.path.exists(os.path.join(store, taskid)):
            ext = ""
        # TBD: is this the correct order (temp < backup) + should temp
        # be considered at all?
        elif getProcPool().os.path.exists(os.path.join(store,
                                                       taskid + TEMP_EXT)):
            ext = TEMP_EXT
        elif getProcPool().os.path.exists(os.path.join(store,
                                                       taskid + BACKUP_EXT)):
            ext = BACKUP_EXT
        else:
            raise se.TaskDirError("loadTask: no such task dir '%s/%s'" %
                                  (store, taskid))
        t._load(store, ext)
        return t

    @threadlocal_task
    def prepare(self, func, *args, **kwargs):
        message = self.error
        try:
            self._incref()
        except se.TaskAborted:
            self._doAbort()
            return
        try:
            self._updateState(State.preparing)
            result = None
            code = 0
            try:
                if func:
                    result = self._run(func, *args, **kwargs)
            except se.TaskAborted as e:
                self.log.info("aborting: %s", e)
                code = e.abortedcode
                message = e.value

            if self.aborting():
                self.log.debug("Prepare: aborted: %s", message)
                self._updateResult(code, "Task prepare failed: %s" %
                                   (message,), "")
                raise self.error

            if self.jobs:
                self.log.debug("Prepare: %s jobs exist, move to acquiring",
                               self.njobs)
                self._updateState(State.acquiring)
                if self.aborting():
                    self.log.error('failed to acquire task %s', self.id)
                    raise self.error
                self.log.debug("returning")
                return dict(uuid=str(self.id))

            self.log.debug("finished: %s", result)
            self._updateResult(0, "OK", result)
            self._updateState(State.finished)
            return result
        finally:
            self._decref()

    @threadlocal_task
    def commit(self, args=None):
        self.log.debug("committing task: %s", self.id)
        try:
            self._incref()
        except se.TaskAborted:
            self._doAbort()
            return
        try:
            self._updateState(State.running)
        finally:
            self._decref()

    @contextmanager
    def abort_callback(self, callback):
        with self._abort_lock:
            if self.aborting():
                aborting = True
            else:
                aborting = False
                self._abort_callbacks.add(callback)

        if aborting:
            callback()

        try:
            yield
        finally:
            with self._abort_lock:
                self._abort_callbacks.discard(callback)

    def _execute_abort_callbacks(self):
        with self._abort_lock:
            self._aborting = True
            abort_callbacks = list(self._abort_callbacks)

        for callback in abort_callbacks:
            try:
                callback()
            except Exception:
                self.log.exception('failure running abort callback')

    def aborting(self):
        return (self._aborting or
                self.state == State.aborting or
                self.state == State.raborting)

    def stop(self, force=False):
        self.log.debug("stopping in state %s (force %s)", self.state, force)
        self._incref(force)
        try:
            if self.state.isDone():
                self.log.debug("Task already stopped (%s), ignoring",
                               self.state)
                return
            elif (self.state.isRecovering() and
                  not force and
                  (self.cleanPolicy == TaskCleanType.auto)):
                self.log.debug("Task (%s) in recovery and force is false, "
                               "ignoring", self.state)
                return

            self._execute_abort_callbacks()
            self._forceAbort = force
        finally:
            self._decref(force)

    @threadlocal_task
    def recover(self, args=None):
        ''' Do not call this function while the task is actually running. this
            method should only be used to recover tasks state after
            (vdsmd) restart.
        '''
        self.log.debug('(recover): recovering: state %s', self.state)
        try:
            self._incref(force=True)
        except se.TaskAborted:
            self._doAbort(True)
            return
        try:
            if self.isDone():
                self.log.debug('(recover): task is done: state %s', self.state)
                return
            # if we are not during recover, just abort
            if self.state.canAbort():
                self.stop()
            # if we waited for recovery - keep waiting
            elif self.state == State.waitrecover:
                pass
            # if we started the recovery - restart it
            elif (self.state == State.racquiring or
                  self.state == State.recovering):
                self._updateState(State.racquiring, force=True)
            # else we were during failed recovery - abort it
            else:
                self.stop(force=True)
        finally:
            self._decref(force=True)
        self.log.debug('(recover): recovered: state %s', self.state)

    def getState(self):
        return str(self.state)

    def getInfo(self):
        return dict(id=self.id, verb=self.name)

    def deprecated_getStatus(self):
        oReturn = {}
        oReturn["taskID"] = self.id
        oReturn["taskState"] = self.state.DEPRECATED_STATE[self.state.state]
        oReturn["taskResult"] = self.state.DEPRECATED_RESULT[self.state.state]
        oReturn["code"] = self.result.code
        oReturn["message"] = self.result.message
        return oReturn

    def getStatus(self):
        oReturn = {}
        oReturn["state"] = {'code': self.result.code,
                            'message': self.result.message}
        oReturn["task"] = {'id': self.id, 'state': str(self.state)}
        oReturn["result"] = self.result.result
        return oReturn

    def getDetails(self):
        return {
            "id": self.id,
            "verb": self.name,
            "state": str(self.state),
            "code": self.result.code,
            "message": self.result.message,
            "result": self.result.result,
            "tag": self.tag
        }

    def getID(self):
        return self.id

    def getTags(self):
        return self.tag

    def __str__(self):
        return str(self.id)

    # FIXME : Use StringIO and enumerate()
    def dumpTask(self):
        s = "Task: %s" % self._dump(self, Task.fields)
        i = 0
        for r in self.recoveries:
            s += " Recovery%d: %s" % (i, self._dump(r, Recovery.fields))
            i += 1
        i = 0
        for j in self.jobs:
            s += " Job%d: %s" % (i, self._dump(j, Job.fields))
            i += 1
        return s

    @misc.logskip("ResourceManager")
    def getExclusiveLock(
        self,
        namespace,
        resName,
        timeout=config.getint('irs',
                              'task_resource_default_timeout')):
        self.resOwner.acquire(namespace,
                              resName,
                              resourceManager.LockType.exclusive,
                              timeout)

    @misc.logskip("ResourceManager")
    def getSharedLock(self,
                      namespace,
                      resName,
                      timeout=config.getint('irs',
                                            'task_resource_default_timeout')):
        self.resOwner.acquire(namespace,
                              resName,
                              resourceManager.LockType.shared,
                              timeout)
