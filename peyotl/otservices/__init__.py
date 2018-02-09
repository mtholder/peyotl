#!/usr/bin/env python
# Some imports to help our py2 code behave like py3
from __future__ import absolute_import, print_function, division
from peyotl import (assure_dir_exists,
                    logger, get_config_object, opentree_config_dir)
import subprocess
import os

OTC_TOL_WS = 'otcws'

def expand_service_nicknames_to_uniq_list(services):
    expansion = {'tnrs': [OTC_TOL_WS, 'ottindexer', ]}
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

class ServiceStatusWrapper(object):
    def __init__(self,
                 service_name,
                 just_launched=False,
                 launcher_pid=None):
        self.service = service_name
        self.just_launched = just_launched
        self.launcher_pid = launcher_pid

def launch_services(services, restart=False):
    # Support for some aliases, like tnrs-> both ottindexer and otcws
    cfg = get_config_object()
    for service in expand_service_nicknames_to_uniq_list(services):
        if is_running(service, cfg):
            if restart:
                _restart_service(service, cfg)
            else:
                logger(__name__).info('{} is already running'.format(service))
        else:
            _launch_service(service, cfg)

def _restart_service(service, cfg):
    logger(__name__).info('Restarting {}'.format(service))
    _stop_service(service, cfg)
    _launch_service(service, cfg)

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
    if service == OTC_TOL_WS:
        launch_otcws(cfg)
    else:
        raise NotImplementedError('launch of {}'.format(service))
    logger(__name__).info('Starting {}...'.format(service))

def launch_otcws(cfg):
    ott_dir = cfg.get_setting(['ott', 'directory'], raise_on_none=True)
    if not os.path.isdir(ott_dir):
        raise RuntimeError('ott/directory setting "{}" is not a directory'.format(ott_dir))
    synth_par = cfg.get_setting(['synthpar', 'directory'], raise_on_none=True)
    if not os.path.isdir(synth_par):
        raise RuntimeError('synthpar/directory setting "{}" is not a directory'.format(synth_par))
    dir = os.path.join(opentree_config_dir()[0], OTC_TOL_WS)
    otc_settings = cfg.get_setting(['otcws'], raise_on_none=True)
    port_num = otc_settings.get('port', 1984)
    num_threads = otc_settings.get('num_threads', 1)
    invoc = ['otc-tol-ws',
             os.path.abspath(ott_dir),
             '--tree-dir={}'.format(os.path.abspath(synth_par)),
             '--port={}'.format(port_num),
             '--num-thread={}'.format(num_threads)
            ]
    proc_status = _launch_daemon('otcws', invoc, dir)
    if proc_status.is_running:
        logger(__name__).info('launched otc-tol-ws. PID={}'.format(proc_status.pid))
    else:
        proc_status.diagnose()

def _launch_daemon(service, command, par_dir):
    assure_dir_exists(par_dir)
    logfile = os.path.join(par_dir, 'log')
    invoc = ['otjoblauncher.py',
               par_dir,
               '',
               logfile,
               logfile,
            ] + command
    p = subprocess.Popen(invoc).pid
    return ServiceStatusWrapper(service, just_launched=True, launcher_pid=p)
