#!/usr/bin/env python
# Some imports to help our py2 code behave like py3
from __future__ import absolute_import, print_function, division
from peyotl import (assure_dir_exists, CfgSettingType,
                    logger, get_config_object, opentree_config_dir)
from peyotl.jobs import launch_job, JobStatusWrapper
from threading import Lock
import subprocess
import logging
from codecs import open
import psutil
import time
import sys
import os

OTC_TOL_WS = 'otcws'
_SLEEP_INTERVAL_FOR_LAUNCH = 0.5
_MAX_SLEEPS = 5


def expand_service_nicknames_to_uniq_list(services):
    expansion = {'tnrs': [OTC_TOL_WS, 'ottindexer', ],
                 'all': [OTC_TOL_WS, 'ottindexer', ],
                }
    seen = set()
    expanded = []
    for service in services:
        norm = service.lower()
        ex_list = expansion.get(norm, [norm])
        if len(ex_list) > 1 or ex_list[0] != norm:
            em = 'Service start for "{}" mapped to start for "{}"'
            em = em.format(service, '", "'.join(ex_list))
            logger(__name__).info(em)
        for s in ex_list:
            if s not in seen:
                expanded.append(s)
    return expanded


_SERVICE_TO_EXE_NAME = {OTC_TOL_WS: 'otc-tol-ws',
                        }
_ALL_SERVICES = list(_SERVICE_TO_EXE_NAME.keys())
_ALL_SERVICES.sort()
_ALL_SERVICES = tuple(_ALL_SERVICES)
ALL_SERVICE_NAMES = ('all', OTC_TOL_WS, 'tnrs')
# Keep in sync with otjobloauncher
_RSTATUS_NAME = ("NOT_LAUNCHED", "NOT_LAUNCHABLE", "RUNNING", "ERROR_EXIT", "COMPLETED", "DELETED")


# noinspection PyClassHasNoInit
class RStatus:
    NOT_LAUNCHED, NOT_LAUNCHABLE, RUNNING, ERROR_EXIT, COMPLETED, DELETED, CONFLICT = range(7)

    @staticmethod
    def to_str(x):
        return _RSTATUS_NAME[x]

def wait_for_fp_or_raise(fp):
    if not os.path.exists(fp):
        for n in range(_MAX_SLEEPS):
            time.sleep(_SLEEP_INTERVAL_FOR_LAUNCH)
            if os.path.exists(fp):
                break
        if not os.path.exists(fp):
            m = "Launch not detected after monitoring filepath {}".format(fp)
            raise RuntimeError(m)


def kill_pid_or_False(pid):
    try:
        proc = psutil.Process(pid)
    except:
        logger(__name__).warn('Process PID={} could not be found.'.format(pid))
        return False
    try:
        proc.kill()
    except:
        logger(__name__).warn('Could not kill PID={}.'.format(pid))
        return False
    return True


def service_status(services):
    if not services:
        services = _ALL_SERVICES
    for s in services:
        ServiceStatusWrapper(s).write_diagnosis(sys.stdout)


def launch_services(services, restart=False):
    # Support for some aliases, like tnrs-> both ottindexer and otcws
    cfg = get_config_object()
    for service in expand_service_nicknames_to_uniq_list(services):
        success = True
        if is_running(service, cfg):
            if restart:
                success = _restart_service(service, cfg)
            else:
                logger(__name__).info('{} is already running'.format(service))
        else:
            success = _launch_service(service, cfg)
        if not success:
            raise RuntimeError("Could not launch {}".format(service))


def stop_services(services, restart=False):
    # Support for some aliases, like tnrs-> both ottindexer and otcws
    cfg = get_config_object()
    for service in expand_service_nicknames_to_uniq_list(services):
        success = True
        ssw = ServiceStatusWrapper(service)
        if ssw.is_running:
            logger(__name__).info('Killing {} ...'.format(service))
            success = ssw.kill()
        else:
            logger(__name__).info('{} is not running'.format(service))
        if not success:
            raise RuntimeError("Could not kill {}".format(service))

def _restart_service(service, cfg):
    logger(__name__).info('Restarting {}'.format(service))
    _stop_service(service, cfg)
    return _launch_service(service, cfg)


def _stop_service(service, cfg):
    raise NotImplementedError('_stop_service')


def is_running(service, cfg):
    """Currently just returns True if the <OTConfigDir>/<service>/pid file exists."""
    ocd = opentree_config_dir()[0]
    p = os.path.join(ocd, service)
    if not os.path.isdir(p):
        return False
    pidfile = os.path.join(p, 'pid')
    return os.path.isfile(pidfile)


def _launch_service(service, cfg):
    logger(__name__).info('Starting {}...'.format(service))
    if service == OTC_TOL_WS:
        rc = launch_otcws(cfg)
    else:
        raise NotImplementedError('launch of {}'.format(service))
    return rc


def launch_otcws(cfg):
    ott_dir = cfg.get_setting(['ott', 'directory'],
                              raise_on_none=True,
                              type_check=CfgSettingType.EXISTING_DIR)
    synth_par = cfg.get_setting(['synthpar', 'directory'],
                                raise_on_none=True,
                                type_check=CfgSettingType.EXISTING_DIR)
    otcfgdir = os.path.join(opentree_config_dir(), OTC_TOL_WS)
    otc_settings = cfg.get_setting(['otcws'], raise_on_none=True)
    port_num = otc_settings.get('port', 1984)
    num_threads = otc_settings.get('num_threads', 1)
    invoc = ['otc-tol-ws',
             os.path.abspath(ott_dir),
             '--tree-dir={}'.format(os.path.abspath(synth_par)),
             '--port={}'.format(port_num),
             '--num-thread={}'.format(num_threads)
             ]
    service = 'otcws'
    try:
        pid = launch_job(otcfgdir, service, None, None, invoc)
    except:
        logger(__name__).exception('Error launching {}'.format(service))
        return False
    proc_status = JobStatusWrapper(service, pid, just_launched=True)
    if proc_status.is_running:
        logger(__name__).info('launched {}. PID={}'.format(service, pid))
        return True
    proc_status.write_diagnosis(out=None)
    return False

