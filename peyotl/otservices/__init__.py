#!/usr/bin/env python
# Some imports to help our py2 code behave like py3
from __future__ import absolute_import, print_function, division
from peyotl import (assure_dir_exists, CfgSettingType,
                    logger, get_config_object, opentree_config_dir)
from threading import Lock
import subprocess
import logging
import codecs
import psutil
import time
import sys
import os

OTC_TOL_WS = 'otcws'
_SLEEP_INTERVAL_FOR_LAUNCH = 0.5
_MAX_SLEEPS = 5


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


_SERVICE_TO_EXE_NAME = {OTC_TOL_WS: 'otc-tol-ws',
                        }
_ALL_SERVICES = list(_SERVICE_TO_EXE_NAME.keys())
_ALL_SERVICES.sort()
_ALL_SERVICES = tuple(_ALL_SERVICES)

# Keep in sync with otjobloauncher
_RSTATUS_NAME = ("NOT_LAUNCHED", "NOT_LAUNCHABLE", "RUNNING", "ERROR_EXIT", "COMPLETED", "DELETED")


# noinspection PyClassHasNoInit
class RStatus:
    NOT_LAUNCHED, NOT_LAUNCHABLE, RUNNING, ERROR_EXIT, COMPLETED, DELETED, CONFLICT = range(7)

    @staticmethod
    def to_str(x):
        return _RSTATUS_NAME[x]


class ServiceStatusWrapper(object):
    def __init__(self,
                 service_name,
                 just_launched=False,
                 launcher_pid=None):
        self.service = service_name
        self.just_launched = just_launched
        self.launcher_pid = launcher_pid
        self._status_dict = None
        self._status_lock = Lock()

    @property
    def service_dir(self):
        return os.path.join(opentree_config_dir(), self.service)

    @property
    def metadata_dir(self):
        return os.path.join(self.service_dir, '.process_metadata')

    @property
    def process_log(self):
        return os.path.join(self.service_dir, 'log')

    @property
    def pid_filename(self):
        return os.path.join(self.metadata_dir, 'pid')

    @property
    def env_filename(self):
        return os.path.join(self.metadata_dir, 'env')

    @property
    def invocation_filename(self):
        return os.path.join(self.metadata_dir, 'invocation')

    @property
    def invocation(self):
        try:
            return codecs.open(self.invocation_filename, 'r', encoding='utf-8').read().strip()
        except:
            return '#UNKNOWN INVOCATION#'

    def _read_pid_or_none(self):
        try:
            return int(codecs.open(self.pid_filename, 'r', encoding='utf-8').read().strip())
        except:
            return None

    def _diagnose_status(self, check_again=True):
        d = {}
        s = os.path.join(self.metadata_dir, 'status')
        if not os.path.isfile(s):
            if self.just_launched:
                for n in range(_MAX_SLEEPS):
                    time.sleep(_SLEEP_INTERVAL_FOR_LAUNCH)
                    if os.path.isfile(s):
                        break
                if not os.path.isfile(s):
                    m = "Launch not detected after monitoring for file at {}".format(s)
                    raise RuntimeError(m)

        content = codecs.open(s, 'r', encoding='utf-8').read().strip()
        try:
            status_index = _RSTATUS_NAME.index(content)
        except ValueError:
            raise RuntimeError('Unknown status in "{}" in "{}"'.format(content, s))
        d['status_idx_from_launcher'] = status_index
        d['status_from_launcher'] = content
        d['status_idx'] = status_index
        d['is_running'] = False
        d['pid'] = None
        if status_index == RStatus.RUNNING:
            # Launcher thinks that the process was launched
            pid = self._read_pid_or_none()
            d['pid'] = pid
            if d['pid'] is None:
                if check_again:
                    # Might have just failed, check now, but don't recurse and keep checking
                    # @TODO should check for joblauncher PID and interrogate it everywhere we
                    #    check_again...
                    return self._diagnose_status(check_again=False)
                else:
                    d['status_idx'] = RStatus.CONFLICT
                    m = '{} indicated RUNNING but {} does not exist'.format(s, self.pid_filename)
                    d['reason'] = m
            else:
                try:
                    procw = psutil.Process(d['pid'])
                except:
                    if check_again:
                        return self._diagnose_status(check_again=False)
                    d['status_idx'] = RStatus.CONFLICT
                    m = '{} indicated RUNNING but PID {} not detected'.format(s, pid)
                    d['reason'] = m
                else:
                    expected_exe = _SERVICE_TO_EXE_NAME[self.service]
                    try:
                        exe = procw.exe()
                    except:
                        # Exception if we ask about some root process, sometime...
                        exe = '<unknown privileged process>'
                    if not exe.endswith(expected_exe):
                        if check_again:
                            return self._diagnose_status(check_again=False)
                        d['status_idx'] = RStatus.CONFLICT
                        m = '{} indicated RUNNING but PID {} is {} instead of {}'
                        d['reason'] = m.format(s, pid, exe, expected_exe)
                    else:
                        d['is_running'] = True
        self._status_dict = d

    def write_diagnosis(self, out=sys.stdout):
        d = self._gen_diagnosis_message()
        if out is None:
            log = logger(__name__)
            for msg, level in d:
                log.log(level, msg)
        else:
            for p in d:
                out.write('{}\n'.format(p[0]))

    def _gen_diagnosis_message(self):
        level = logging.WARN
        s = self.status
        si = s['status_idx']
        if si == RStatus.CONFLICT:
            msg = 'internal conflict in {} service status: {}'
            msg = msg.format(self.service, s['reason'])
        elif si == RStatus.NOT_LAUNCHED:
            msg = '{} has not been launched by the job launcher'.format(self.service)
        elif si == RStatus.NOT_LAUNCHABLE:
            msg = '{} could not be launched. Perhaps the executable is not on the path. ' \
                  'Try:\n    {}\nand see the env in {}'
            msg = msg.format(self.service, self.invocation, self.env_filename)
            level = logging.ERROR
        elif si == RStatus.RUNNING:
            level = logging.INFO
            msg = '{} is running with PID={}'.format(self.service, self.pid)
        elif si == RStatus.ERROR_EXIT:
            msg = '{} is exited with an error. Check "{}" and "{}"'
            msg = msg.format(self.service, self.process_log, self.metadata_dir)
        elif si == RStatus.COMPLETED:
            msg = '{} completed and is no longer running'.format(self.service)
        elif si == RStatus.DELETED:
            level = logging.ERROR
            msg = 'Unexpected DELETED status for {}'.format(self.service)
        else:
            level = logging.ERROR
            msg = 'Unknown status: {}'.format(s['status_from_launcher'])
        return [(msg, level)]

    @property
    def status(self):
        if self._status_dict is None:
            with self._status_lock:
                if self._status_dict is None:
                    self._diagnose_status()
        return self._status_dict

    @property
    def is_running(self):
        return self.status['is_running']

    @property
    def pid(self):
        return self.status['pid']


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
    proc_status = _launch_daemon('otcws', invoc, otcfgdir)
    if proc_status.is_running:
        logger(__name__).info('launched otc-tol-ws. PID={}'.format(proc_status.pid))
        return True
    proc_status.write_diagnosis(out=None)
    return False


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
