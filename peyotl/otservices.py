#!/usr/bin/env python
# Some imports to help our py2 code behave like py3
from __future__ import absolute_import, print_function, division

import os
import sys

from peyotl import (CfgSettingType,
                    logger, get_config_object, opentree_config_dir)
from peyotl.jobs import launch_job, JobStatusWrapper
from peyotl.jobs import (ALL_SERVICES, ALL_SERVICE_NAMES,
                         OTC_TOL_WS,)


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


def service_status(services):
    if not services:
        services = ALL_SERVICES
    out = sys.stdout
    for s in services:
        if 0 == JobStatusWrapper(s).write_diagnosis(out):
            out.write('{} not running\n'.format(s))



def launch_services(services, restart=False):
    # Support for some aliases, like tnrs-> both ottindexer and otcws
    cfg = get_config_object()
    for service in expand_service_nicknames_to_uniq_list(services):
        success = True
        if is_running(service):
            if restart:
                success = _restart_service(service, cfg)
            else:
                logger(__name__).info('{} is already running'.format(service))
        else:
            success = _launch_service(service, cfg)
        if not success:
            raise RuntimeError("Could not launch {}".format(service))


def stop_services(services):
    for service in expand_service_nicknames_to_uniq_list(services):
        _stop_service(service)


def _restart_service(service, cfg):
    logger(__name__).info('Restarting {}'.format(service))
    _stop_service(service)
    return _launch_service(service, cfg)


def _stop_service(service):
    success = True
    ssw = JobStatusWrapper(service)
    if ssw.is_running:
        logger(__name__).info('Killing {} ...'.format(service))
        success = ssw.kill()
    else:
        logger(__name__).info('{} is not running'.format(service))
    if not success:
        raise RuntimeError("Could not kill {}".format(service))


def is_running(service):
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
        pid = launch_job(service, None, None, invoc)
    except:
        logger(__name__).exception('Error launching {}'.format(service))
        return False
    proc_status = JobStatusWrapper(service, pid, just_launched=True)
    if proc_status.is_running:
        logger(__name__).info('launched {}. PID={}'.format(service, pid))
        return True
    proc_status.write_diagnosis(out=None)
    return False
