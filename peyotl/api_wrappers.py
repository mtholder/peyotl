#!/usr/bin/env python
# Some imports to help our py2 code behave like py3
from __future__ import absolute_import, print_function, division
from enum import Enum
import os
from .utility import get_config_object, logger
from .jobs import JobStatusWrapper, launch_detached_service

class WrapperMode(Enum):
    PURE_IMPL = 1 # API is implemented by calls to python code on client machine
    REMOTE_WS = 2
    LOCAL_WS = 3

class APIWrapper(object):
    def __init__(self, service):
        self._service = service
        self._mode = None
        self._cfg = get_config_object()

    @property
    def service(self):
        return self._service

    @property
    def mode(self):
        assert self._mode is not None
        return self._mode

    def launch_service(self):
        self._assure_not_remote('launch_service')
        if self.mode == WrapperMode.PURE_IMPL:
            return True
        return self._launch_local_service(self)

    def stop_service(self):
        self._assure_not_remote('stop_service')
        if self.mode == WrapperMode.PURE_IMPL:
            return True
        service = self.service
        ssw = JobStatusWrapper(service)
        if ssw.is_running:
            logger(__name__).info('Killing {} ...'.format(service))
            if not ssw.kill():
                raise RuntimeError("Could not kill {}".format(service))
        else:
            logger(__name__).info('{} is not running'.format(service))
        return True

    def run_tests(self, out):
        '''Returns (all_tests_run, all_run_tests_passed) tuple'''
        hca, hcp = self._run_hard_coded_tests(out)
        setting_address = ['code_repos', 'parent']
        cr = self._cfg.get_setting(['code_repos', 'parent'])
        if cr is None:
            logger(__name__).warn('Tests for {} skipped because the "code_repos" section of the '
                                  'configuration file is missing. That makes it impossible for'
                                  'peyotl to find the repositories holding '
                                  'tFalse, Trueests.'.format(self.service))
            return False, hcp
        if not os.path.isdir(cr):
            setting_address.append(self.service)
            raise RuntimeError('The {}/{} setting "{}" should be a directory.'.format(*setting_address))
        sapt_dir = os.path.join(cr, 'shared-api-tests')
        if not os.path.isdir(sapt_dir):
            logger(__name__).warn('Tests for {} skipped because the "{}" directory'
                                  ' was not found.'.format(self.service))
            return False, hcp
        a, p = self._run_shared_api_tests(sapt_dir, out)
        return (a and hca, p and hcp)

    def _run_hard_coded_tests(self, out):
        return True, True


    def _assure_not_remote(self, action_str):
        if self.mode == WrapperMode.REMOTE_WS:
            m = "{} unavailable because {} is configured to be a wrapper around " \
                "remote web service."
            raise ValueError(m.format(action_str, self._service))

    def _launch_local_service(self):
        service = self.service
        invoc = self._get_local_invocation()
        try:
            pid = launch_detached_service(service, invoc)
        except:
            logger(__name__).exception('Error launching {}'.format(service))
            return False
        proc_status = JobStatusWrapper(service, pid, just_launched=True)
        if proc_status.is_running:
            logger(__name__).info('launched {}. PID={}'.format(service, pid))
            return True
        proc_status.write_diagnosis(out=None)
        return False

    def _run_shared_api_tests(self, repo_dir, out):
        raise NotImplementedError("_run_shared_api_tests is a pure virtual of APIWrapper")

    def _get_local_invocation(self):
        raise NotImplementedError("_get_local_invocation is a pure virtual of APIWrapper")

    def _call_method_from_shared_test(self, out, name, bound_method, input, expected_out):
        pref = '{} test {}'.format(type(self), name)
        try:
            result = bound_method(**input)
        except Exception as x:
            ee = expected_out.get('parameters_error')
            if ee:
                if str(type(x)) == ee[0]:
                    return True
                m = '{}. Expected exception of type {} but got {}.\n'.format(pref, ee[0], type(x))
            else:
                m = '{}. Expected success, but got exception: {}\n'.format(pref, str(x))
            out.write(m)
            return False
        NotImplementedError('testing of results')



