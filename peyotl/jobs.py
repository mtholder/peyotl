#!/usr/bin/env python
# Some imports to help our py2 code behave like py3
from __future__ import absolute_import, print_function, division
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from peyotl import (assure_dir_exists, CfgSettingType,
                    logger, get_config_object, opentree_config_dir)
from threading import Lock
import subprocess
from codecs import open
import logging
from codecs import open
import psutil
import time
import sys
import os

_SLEEP_INTERVAL_FOR_LAUNCH = 0.5
_MAX_SLEEPS = 5

Base = declarative_base()
class Process(Base):
    __tablename__ = 'processes'

    id = Column(Integer, primary_key=True)
    name = Column(String)
    pid = Column(Integer)
    wdir = Column(String)
    status = Column(Integer)
    comment = Column(String)

    def __repr__(self):
        t = '<Process(name={n!r}, pid={p!r}, wdir={w!r}, status={s!r}, comment={c!r})>'
        return t.format(n=self.name, p=self.pid, w=self.wdir, s=self.status, c=self.comment)

_engine = None
_engine_lock = Lock()

def _get_db_engine(parent_dir):
    global _engine
    if _engine is not None:
        return _engine
    with _engine_lock:
        if _engine is None:
            proc_db_fp = os.path.abspath(os.path.join(parent_dir), 'processes.db')
            initialize = not os.path.isfile(proc_db_fp)
            e = create_engine('sqlite:///{}'.format(proc_db_fp))
            if initialize:
                Base.metadata.create_all(e)
            _engine = e
    return _engine



_RSTATUS_NAME = ("NOT_LAUNCHED",
                 "NOT_LAUNCHABLE",
                 "RUNNING",
                 "ERROR_EXIT",
                 "COMPLETED",
                 "KILLED",
                 "KILL_SENT")


# noinspection PyClassHasNoInit
class RStatus:
    NOT_LAUNCHED, NOT_LAUNCHABLE, RUNNING, ERROR_EXIT, COMPLETED, KILLED, KILL_SENT = range(7)

    @staticmethod
    def to_str(x):
        return _RSTATUS_NAME[x]


def shell_escape_arg(s):
    return "\\ ".join(s.split())


def launch_job(config_dir, name, stdout_fp, stderr_fp, invocation):
    e = _get_db_engine(parent_dir=config_dir)
    session = sessionmaker(bind=e)
    # Generate a working dir, which will be the config_dir/name or config_dir/name_1
    working_dir_base = os.path.join(config_dir, name)
    working_dir = working_dir_base
    suffix = 1
    # Dealing with race conditions caused by our desire to have a simple numbering suffix scheme
    while True:
        while session.query(Process.wdir).filter_by(wdir=working_dir).count() > 0:
            working_dir = '{}_{}'.format(working_dir_base, suffix)
            suffix += 1
        pdb = Process(name=name, pid=-1, wdir=working_dir, status=RStatus.NOT_LAUNCHED, comment='')
        session.add(pdb)
        if session.query(Process.wdir).filter_by(wdir=working_dir).count() > 1:
            session.rollback()
        session.commit()
        if session.query(Process.wdir).filter_by(wdir=working_dir).count() > 1:
            session.delete(pdb)
        else:
            break
    launcher_err_stream = None
    try:
        assure_dir_exists(working_dir)
        if stdout_fp is None:
            stdout_fp = os.path.join(working_dir, 'stdout')
        else:
            stdout_fp = os.path.abspath(stdout_fp)
        outf = open(stdout_fp, 'a', encoding='utf-8')
        if stderr_fp is None:
            stderr_fp = os.path.join(working_dir, 'stderr')
        elif stderr_fp == subprocess.STDOUT:
            stderr_fp = stdout_fp
        else:
            stderr_fp = os.path.abspath(stderr_fp)
        if stderr_fp == stdout_fp:
            errf = subprocess.STDOUT
        else:
            errf = open(stderr_fp, "a", encoding='utf-8')
        md = os.path.join(working_dir, ".process_metadata")
        if not os.path.exists(md):
            os.mkdir(md)
        lem = os.path.join(md, 'launcher_err.txt')
        launcher_err_stream = open(lem, 'w', encoding='utf-8')
        escaped_invoc = ' '.join([shell_escape_arg(i) for i in invocation])
        with open(os.path.join(md, 'invocation'), 'w', encoding='utf-8') as invout:
            invout.write("{i}\n".format(i=escaped_invoc))
        with open(os.path.join(md, 'stdoe'), 'w', encoding='utf-8') as ioout:
            ioout.write("{o}\n{e}}\n".format(o=stdout_fp, e=stderr_fp)
        with open(os.path.join(md, "env"), "w", encoding='utf-8') as eout:
            for k, v in os.environ.items():
                eout.write("export {}='{}'\n".format(k, "\'".join(v.split("'"))))
    except:
        # Could not even get to the launch step... delete this process entry in the db...
        session.delete(pdb)
        session.commit()
        raise
    try:
        proc = subprocess.Popen(invocation, stdin=open(os.devnull, 'r'), stdout=outf, stderr=errf)
    except:
        pdb.status = RStatus.NOT_LAUNCHABLE
        session.commit()
        raise
    else:
        pdb.status = RStatus.RUNNING
        pdb.pid = proc.pid
        session.commit()
        #@TODO: only works for very short stdin that won't fill a buffer...
        return proc.pid



class JobStatusWrapper(object):
    def __init__(self,
                 service_name,
                 pid=None,
                 just_launched=False):
        self.service = service_name
        self.just_launched = just_launched
        self.expected_pid = pid
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
    def pid_filename(self):
        return os.path.join(self.metadata_dir, 'launcher_pid')

    @property
    def env_filename(self):
        return os.path.join(self.metadata_dir, 'env')

    @property
    def invocation_filename(self):
        return os.path.join(self.metadata_dir, 'invocation')

    @property
    def invocation(self):
        try:
            return open(self.invocation_filename, 'r', encoding='utf-8').read().strip()
        except:
            return '#UNKNOWN INVOCATION#'

    def _read_pid_or_none(self):
        try:
            return int(open(self.pid_filename, 'r', encoding='utf-8').read().strip())
        except:
            return None

    def _read_launcher_pid_or_none(self):
        try:
            return int(open(self.launcher_pid_filename, 'r', encoding='utf-8').read().strip())
        except:
            return None

    def kill(self):
        s = self.status
        if s['status_idx'] == RStatus.RUNNING:
            launcher_pid = s['launcher_pid']
            if launcher_pid:
                if kill_pid_or_False(launcher_pid):
                    logger(__name__).info("Killed job launcher PID={}".format(launcher_pid))
                    return
                logger(__name__).info("Could not kill job launcher PID={}".format(launcher_pid))
            spid = s['pid']
            if kill_pid_or_False(spid):
                logger(__name__).info("Killed {} PID={}".format(self.service, spid))
                return
            logger(__name__).info("Could not kill {} PID={}".format(self.service, spid))
        else:
            logger(__name__).info("{} is not running".format(self.service))

    def _diagnose_status(self, check_again=True):
        d = {'is_running': False,
             'pid': None,
             'launcher_pid': None
            }
        if self.just_launched and not os.path.isdir(self.metadata_dir):
            wait_for_fp_or_raise(self.metadata_dir)
        if self.just_launched and not os.path.isdir(self.metadata_dir):
            d['status_idx_from_launcher'] = RStatus.NOT_LAUNCHED
            d['status_from_launcher'] = 'Lack of metadatadir indicates service has never been launched'
            d['status_idx'] = RStatus.NOT_LAUNCHED
        else:
            s = os.path.join(self.metadata_dir, 'status')
            if self.just_launched:
                wait_for_fp_or_raise(s)
            content = open(s, 'r', encoding='utf-8').read().strip()
            try:
                status_index = _RSTATUS_NAME.index(content)
            except ValueError:
                raise RuntimeError('Unknown status in "{}" in "{}"'.format(content, s))
            d['status_idx_from_launcher'] = status_index
            d['status_from_launcher'] = content
            d['status_idx'] = status_index
            d['launcher_pid'] = self._read_launcher_pid_or_none()
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

