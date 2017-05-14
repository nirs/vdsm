#
# Copyright 2016-2017 Red Hat, Inc.
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

import sys

import six

from vdsm import concurrent
from vdsm.common import api
from vdsm.common import exception
from vdsm.common import response
from vdsm.common.threadlocal import vars

from testlib import Sigargs
from testlib import VdsmTestCase as TestCaseBase


class TestApiMethod(TestCaseBase):

    def test_preserve_signature(self):
        vm = FakeVM()
        args = Sigargs(vm.fail)
        self.assertEqual(args.args, ['self', 'exc'])
        self.assertEqual(args.varargs, None)
        self.assertEqual(args.keywords, None)


class TestResponse(TestCaseBase):

    def setUp(self):
        self.vm = FakeVM()

    def test_success_without_return(self):
        res = self.vm.succeed()
        self.assertEqual(res, response.success())

    def test_success_with_return_dict(self):
        vmList = ['foobar']
        res = self.vm.succeed_with_return({'vmList': vmList})
        self.assertEqual(response.is_error(res), False)
        self.assertEqual(res['vmList'], vmList)

    def test_success_with_args(self):
        args = ("foo", "bar")
        res = self.vm.succeed_with_args(*args)
        self.assertEqual(response.is_error(res), False)
        self.assertEqual(res['args'], args)

    def test_success_with_kwargs(self):
        kwargs = {"foo": "bar"}
        res = self.vm.succeed_with_kwargs(**kwargs)
        self.assertEqual(res['kwargs'], kwargs)
        self.assertEqual(response.is_error(res), False)

    def test_success_with_wrong_return(self):
        vmList = ['foobar']  # wrong type as per @api.method contract
        self.assertRaises(TypeError,
                          self.vm.succeed_with_return,
                          vmList)

    def test_success_with_return_dict_override_message(self):
        message = 'this message overrides the default'
        res = self.vm.succeed_with_return({'message': message})
        self.assertEqual(response.is_error(res), False)
        self.assertEqual(res['status']['message'], message)

    def test_fail_with_vdsm_exception(self):
        exc = exception.NoSuchVM()
        res = self.vm.fail(exc)
        expected = exception.NoSuchVM().response()
        self.assertEqual(res, expected)

    def test_fail_with_general_exception(self):
        exc = ValueError()
        res = self.vm.fail(exc)
        expected = exception.GeneralException(str(exc)).response()
        self.assertEqual(res, expected)

    def test_passthrough(self):
        foo = 'foo'
        res = self.vm.succeed_passthrough(foo=foo)
        self.assertEqual(res, response.success(foo=foo))


class FakeVM(object):

    @api.method
    def fail(self, exc):
        raise exc

    @api.method
    def succeed(self):
        pass

    @api.method
    def succeed_with_return(self, ret):
        return ret

    @api.method
    def succeed_with_args(self, *args):
        return {"args": args}

    @api.method
    def succeed_with_kwargs(self, **kwargs):
        return {"kwargs": kwargs}

    @api.method
    def succeed_passthrough(self, foo):
        return response.success(foo=foo)


class TestLoggedWithContext(TestCaseBase):

    def test_success(self):
        # TODO: test logged message
        ctx = api.Context("flow_id", "1.2.3.4", 5678)
        result = run_with_context(ctx, Logged().succeed, "a", b=1)
        self.assertEqual(result, (("a",), {"b": 1}))

    def test_fail(self):
        # TODO: test logged message
        ctx = api.Context("flow_id", "1.2.3.4", 5678)
        error = RuntimeError("Expected failure")
        with self.assertRaises(RuntimeError) as e:
            run_with_context(ctx, Logged().fail, error)
        self.assertIs(e.exception, error)


class TestLoggedWithoutContext(TestCaseBase):

    def test_success(self):
        # TODO: test logged message
        result = run_with_context(None, Logged().succeed, "a", b=1)
        self.assertEqual(result, (("a",), {"b": 1}))

    def test_fail(self):
        # TODO: test logged message
        error = RuntimeError("Expected failure")
        with self.assertRaises(RuntimeError) as e:
            run_with_context(None, Logged().fail, error)
        self.assertIs(e.exception, error)


def run_with_context(ctx, func, *args, **kwargs):
    """
    Run func in another thread with optional ctx set in vars.context.

    Return the function result or raises the original exceptions raised by
    func.
    """
    result = [None]

    def run():
        if ctx:
            vars.context = ctx
        try:
            result[0] = (True, func(*args, **kwargs))
        except:
            result[0] = (False, sys.exc_info())

    t = concurrent.thread(run)
    t.start()
    t.join()

    ok, value = result[0]
    if not ok:
        six.reraise(*value)
    return value


class Logged(object):

    @api.logged("test")
    def succeed(self, *args, **kwargs):
        return args, kwargs

    @api.logged("test")
    def fail(self, exc):
        raise exc
