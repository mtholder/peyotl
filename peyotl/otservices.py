#!/usr/bin/env python
# Some imports to help our py2 code behave like py3
from __future__ import absolute_import, print_function, division

import os
import sys

from peyotl import (logger, opentree_config_dir)
from peyotl.jobs import (ALL_TRUE_SERVICE_NAMES,
                         )
from peyotl.jobs import (expand_service_nicknames_to_uniq_list,
                         JobStatusWrapper,
                         )
from peyotl.utility import reverse_line_reader_gen


def service_status(services):
    if not services:
        services = ALL_TRUE_SERVICE_NAMES
    out = sys.stdout
    checked = set()
    for sn in services:
        for s in expand_service_nicknames_to_uniq_list(sn):
            if s in checked:
                continue
            if 0 == JobStatusWrapper(s).write_diagnosis(out):
                out.write('{} not running\n'.format(s))
            checked.add(s)
    return True


def launch_services(services, restart=False):
    # Support for some aliases, like tnrs-> both ottindexer and otcws
    for service in expand_service_nicknames_to_uniq_list(services):
        success = True
        if is_running(service):
            if restart:
                success = _restart_service(service)
            else:
                logger(__name__).info('{} is already running'.format(service))
        else:
            success = _launch_service(service)
        if not success:
            raise RuntimeError("Could not launch {}".format(service))
    return True


def write_service_log_tail(out, service_nick, n=10):
    for service in expand_service_nicknames_to_uniq_list(service_nick):
        _write_log_tail(out, service, n)


def test_service(out, service_nick):
    all_run, all_passed = True, True
    for service in expand_service_nicknames_to_uniq_list(service_nick):
        r, p = _test_service(out, service)
        all_run = all_run and r
        all_passed = all_passed and p
    return all_run, all_passed


def stop_services(services):
    success = True
    for service in expand_service_nicknames_to_uniq_list(services):
        success = _stop_service(service) and success
    return success


def _restart_service(service):
    logger(__name__).info('Restarting {}'.format(service))
    _stop_service(service)
    return _launch_service(service)


def _stop_service(service):
    from .api import SERVICE_NAME_TO_WRAPPER
    wrapper = SERVICE_NAME_TO_WRAPPER.get(service)
    if wrapper is None:
        raise NotImplementedError('stop of {}'.format(service))
    return wrapper().stop_service()


def _test_service(out, service):
    """Returns (all_tests_run, all_test_ran_passed)."""
    from .api import SERVICE_NAME_TO_WRAPPER
    wrapper = SERVICE_NAME_TO_WRAPPER.get(service)
    if wrapper is None:
        raise NotImplementedError('test of {}'.format(service))
    return wrapper().run_tests(out)


def _write_log_tail(out, service, n):
    jsw = JobStatusWrapper(service)
    tp = []
    for index, line in enumerate(reverse_line_reader_gen(jsw.log_filepath)):
        if index >= n:
            break
        tp.append('{}\n'.format(line))
    for line in reversed(tp):
        out.write(line)


def is_running(service):
    """Currently just returns True if the <OTConfigDir>/<service>/pid file exists."""
    ocd = opentree_config_dir()[0]
    p = os.path.join(ocd, service)
    if not os.path.isdir(p):
        return False
    pidfile = os.path.join(p, 'pid')
    return os.path.isfile(pidfile)


def _launch_service(service):
    logger(__name__).info('Starting {}...'.format(service))
    from .api import SERVICE_NAME_TO_WRAPPER
    wrapper = SERVICE_NAME_TO_WRAPPER.get(service)
    if wrapper is None:
        raise NotImplementedError('launch of {}'.format(service))
    return wrapper().launch_service()
