#!/usr/bin/env python
# Some imports to help our py2 code behave like py3
from __future__ import absolute_import, print_function, division
from enum import Enum
from .utility import logger
from .jobs import JobStatusWrapper, launch_detached_service

class WrapperMode(Enum):
    PURE_IMPL = 1 # API is implemented by calls to python code on client machine
    REMOTE_WS = 2
    LOCAL_WS = 3

class APIWrapper(object):
    def __init__(self, service):
        self._service = service
        self._mode = None

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

    def _get_local_invocation(self):
        raise NotImplementedError("_get_local_invocation is a pure virtual of APIWrapper")