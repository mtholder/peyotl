#!/usr/bin/env python
# Some imports to help our py2 code behave like py3
from __future__ import absolute_import, print_function, division

import re
import os
import sys
import time
import subprocess
import logging
from codecs import open
from threading import Lock
from tempfile import mkdtemp

import psutil
from sqlalchemy import create_engine, Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from sqlalchemy.orm.exc import NoResultFound

from peyotl import (assure_dir_exists, logger, opentree_config_dir, is_str_type)

PROCESS_METADATA = ".process_metadata"
_SLEEP_INTERVAL_FOR_LAUNCH = 0.5
_MAX_SLEEPS = 5

Base = declarative_base()

OTC_TOL_WS = 'otcws'
OTT_NAME_INDEXER = 'ottindexer'
_SERVICE_TO_EXE_NAME = {OTC_TOL_WS: 'otc-tol-ws',
                        }
ALL_SERVICES = list(_SERVICE_TO_EXE_NAME.keys())
ALL_SERVICES.sort()
ALL_SERVICES = tuple(ALL_SERVICES)
_SERVICE_SET_NAME_TO_LIST = {
    'tnrs': [OTC_TOL_WS, OTT_NAME_INDEXER],
    'all': [OTC_TOL_WS, OTT_NAME_INDEXER, ],
}
ALL_TRUE_SERVICE_NAMES = (OTC_TOL_WS, OTT_NAME_INDEXER,)
ALL_SERVICE_NAMES = ('all', OTC_TOL_WS, OTT_NAME_INDEXER, 'tnrs',)


def expand_service_nicknames_to_uniq_list(services):
    seen = set()
    expanded = []
    if is_str_type(services):
        services = [services]
    for service in services:
        norm = service.lower()
        ex_list = _SERVICE_SET_NAME_TO_LIST.get(norm, [norm])
        if len(ex_list) > 1 or ex_list[0] != norm:
            em = 'Service start for "{}" mapped to start for "{}"'
            em = em.format(service, '", "'.join(ex_list))
            logger(__name__).info(em)
        for s in ex_list:
            if s not in seen:
                expanded.append(s)
    return expanded


# noinspection PyClassHasNoInit
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


# noinspection PyClassHasNoInit
class ArchivedProcess(Base):
    __tablename__ = 'archived_processes'
    id = Column(Integer, primary_key=True)
    name = Column(String)
    archivedir = Column(String)
    status = Column(Integer)


_engine = None
_engine_lock = Lock()


def _get_db_engine(parent_dir):
    global _engine
    if _engine is not None:
        return _engine
    with _engine_lock:
        if _engine is None:
            proc_db_fp = os.path.abspath(os.path.join(parent_dir, 'processes.db'))
            initialize = not os.path.isfile(proc_db_fp)
            e = create_engine('sqlite:///{}'.format(proc_db_fp))
            if initialize:
                Base.metadata.create_all(e)
            _engine = e
    return _engine


_RSTATUS_NAME = ("NOT_LAUNCHED",  # Launch has been requested, but process spawning is not complete
                 "NOT_LAUNCHABLE",  # Error while trying to launch (e.g. exe not on path)
                 "RUNNING",
                 "ERROR_EXIT",
                 "COMPLETED",
                 "KILLED",  # status had been KILL_SENT, but process is not longer running
                 "KILL_SENT",  # a wrapper's kill() method has been triggered
                 "BEING_ARCHIVED",
                 "MISSING_PRESUMED_ARCHIVED",
                 "ARCHIVED",
                 "MISSING_NOT_ARCHIVED",
                 )


# noinspection PyClassHasNoInit
class RStatus:
    NOT_LAUNCHED = 0
    NOT_LAUNCHABLE = 1
    RUNNING = 2
    ERROR_EXIT = 3
    COMPLETED = 4
    KILLED = 5
    KILL_SENT = 6
    BEING_ARCHIVED = 7
    MISSING_PRESUMED_ARCHIVED = 8
    ARCHIVED = 9
    MISSING_NOT_ARCHIVED = 10

    @staticmethod
    def to_str(x):
        return _RSTATUS_NAME[x]


_active_status_codes = frozenset([RStatus.NOT_LAUNCHED, RStatus.RUNNING, RStatus.KILL_SENT])


def shell_escape_arg(s):
    return "\\ ".join(s.split())


def get_new_session(config_dir=None):
    if config_dir is None:
        config_dir = opentree_config_dir()
    e = _get_db_engine(parent_dir=config_dir)
    return sessionmaker(bind=e)()


def _do_wait_for_proc_launch(proc_id, session, name):
    for i in range(_MAX_SLEEPS):
        try:
            proc = session.query(Process).filter_by(id=proc_id).one()
        except NoResultFound:
            return RStatus.MISSING_PRESUMED_ARCHIVED, None
        else:
            if proc.status != RStatus.NOT_LAUNCHED:
                return proc.status, proc
        time.sleep(_SLEEP_INTERVAL_FOR_LAUNCH)
    wt = _MAX_SLEEPS * _SLEEP_INTERVAL_FOR_LAUNCH
    raise RuntimeError("Launch of {} not detected after {} seconds".format(name, wt))


def _verify_status(proc, session):
    if proc.status in _active_status_codes:
        while proc.status == RStatus.NOT_LAUNCHED:
            ns, proc = _do_wait_for_proc_launch(proc.id, session, proc.name)[1]
            if proc is None:
                return ns, None
        if proc.status in _active_status_codes:
            pid = proc.pid
            try:
                procw = psutil.Process(pid)
            except:
                if proc.status == RStatus.KILL_SENT:
                    proc.status = RStatus.KILLED
                else:
                    proc.status = RStatus.COMPLETED
                session.commit()
                return proc.status, proc
            expected_exe = _SERVICE_TO_EXE_NAME[proc.name]
            try:
                exe = procw.exe()
            except:
                # Exception if we ask about some root process, sometime...
                exe = '<unknown privileged process>'
            if not exe.endswith(expected_exe):
                if proc.status == RStatus.KILL_SENT:
                    proc.status = RStatus.KILLED
                else:
                    proc.status = RStatus.COMPLETED
                session.commit()
            return proc.status, proc
    # Might want to check if non-running process have been "ARCHIVED"?
    return proc.status, proc


def _is_active_processdb(proc, session):
    return _verify_status(proc, session)[0] in _active_status_codes


def _archive(proc, session):
    ap = None
    if proc.status != RStatus.BEING_ARCHIVED:
        prev_status = proc.status
        proc.status = RStatus.BEING_ARCHIVED
        session.commit()
        arch_par_dir = os.path.join(opentree_config_dir(),
                                    'service_log_archives',
                                    proc.name)
        assure_dir_exists(arch_par_dir)
        arch_dir = mkdtemp(dir=arch_par_dir)
        if os.path.isdir(proc.wdir):
            os.rename(proc.wdir, arch_dir)
            ap = ArchivedProcess(name=proc.name, archivedir=arch_dir, status=prev_status)
            session.add(ap)
        session.delete(proc)
        session.commit()
    return ap


def get_processdb_wrapper_for_active(session, name, working_dir=None):
    to_move = []
    to_return = []
    if working_dir:
        matches = session.query(Process).filter_by(wdir=working_dir).all()
    else:
        matches = session.query(Process).filter_by(name=name).all()
    for proc in matches:
        if _is_active_processdb(proc, session):
            to_return.append(proc)
        else:
            to_move.append(proc)
    r = []
    for proc in to_move:
        if os.path.isdir(proc.wdir):
            a = _archive(proc, session)
            if a is not None:
                r.append(a)
    if bool(to_return):
        r.extend(to_return)
    return r


def get_status_proc_for_pid(session, pid):
    try:
        proc = session.query(Process).filter_by(pid=pid).one()
    except:
        return RStatus.MISSING_PRESUMED_ARCHIVED, None
    return _verify_status(proc, session)


def launch_detached_service(name, invocation):
    config_dir = opentree_config_dir()
    session = get_new_session(config_dir)
    # Generate a working dir, which will be the config_dir/name or config_dir/name_1
    working_dir_base = os.path.join(config_dir, name)
    working_dir = working_dir_base
    suffix = 1
    # Dealing with race conditions caused by our desire to have a simple numbering suffix scheme
    while True:
        active_at_dir = get_processdb_wrapper_for_active(session, name, working_dir)
        while active_at_dir:
            working_dir = '{}_{}'.format(working_dir_base, suffix)
            suffix += 1
            active_at_dir = get_processdb_wrapper_for_active(session, name, working_dir)

        pdb = Process(name=name, pid=-1, wdir=working_dir, status=RStatus.NOT_LAUNCHED, comment='')
        session.add(pdb)
        if session.query(Process).filter_by(wdir=working_dir).count() > 1:
            session.rollback()
        session.commit()
        if session.query(Process).filter_by(wdir=working_dir).count() > 1:
            session.delete(pdb)
        else:
            break
    try:
        assure_dir_exists(working_dir)
        # if stdout_fp is None:
        stdout_fp = os.path.join(working_dir, 'log'.format(name))
        # else:
        #    stdout_fp = os.path.abspath(stdout_fp)
        outf = open(stdout_fp, 'a', encoding='utf-8')
        # if stderr_fp is None:
        #    stderr_fp = os.path.join(working_dir, 'err'.format(name))
        # elif stderr_fp == subprocess.STDOUT:
        errf = subprocess.STDOUT
        stderr_fp = stdout_fp
        # else:
        #    stderr_fp = os.path.abspath(stderr_fp)
        # if stderr_fp == stdout_fp:
        #    errf = subprocess.STDOUT
        # else:
        #    errf = open(stderr_fp, "a", encoding='utf-8')
        md = os.path.join(working_dir, PROCESS_METADATA)
        if not os.path.exists(md):
            os.mkdir(md)
        escaped_invoc = ' '.join([shell_escape_arg(i) for i in invocation])
        with open(os.path.join(md, 'invocation'), 'w', encoding='utf-8') as invout:
            invout.write("{i}\n".format(i=escaped_invoc))
        with open(os.path.join(md, 'stdoe'), 'w', encoding='utf-8') as ioout:
            ioout.write("{o}\n{e}\n".format(o=stdout_fp, e=stderr_fp))
        with open(os.path.join(md, "env"), "w", encoding='utf-8') as eout:
            for k, v in os.environ.items():
                eout.write("export {}='{}'\n".format(k, "\'".join(v.split("'"))))
    except:
        # Could not even get to the launch step... delete this process entry in the db...
        session.delete(pdb)
        session.commit()
        raise
    try:
        proc = subprocess.Popen(invocation,
                                stdin=open(os.devnull, 'r'),
                                stdout=outf,
                                stderr=errf,
                                cwd=working_dir)
    except:
        pdb.status = RStatus.NOT_LAUNCHABLE
        session.commit()
        raise
    else:
        pdb.status = RStatus.RUNNING
        pdb.pid = proc.pid
        session.commit()
        # @TODO: only works for very short stdin that won't fill a buffer...
        return proc.pid


_OTC_READY_PAT = re.compile(r'.*Service is ready\. PID is [0-9]+')


def _is_serving(proc):
    if proc.status not in _active_status_codes:
        return False
    if proc.name == OTC_TOL_WS:
        with open(os.path.join(proc.wdir, 'log'), 'r', encoding='utf-8') as logf:
            for line in logf:
                if _OTC_READY_PAT.match(line):
                    return True
        return False
    else:
        m = "is_serving not implemented for {}".format(proc.name)
        logger(__name__).error('NotImplementedError: {}'.format(m))
        raise NotImplementedError(m)


class JobStatusWrapper(object):
    def __init__(self,
                 service_name,
                 pid=None,
                 just_launched=False):
        self.service = service_name
        self.just_launched = just_launched
        self.expected_pid = pid
        self._proc_list = None
        self._session = None
        self._status_lock = Lock()

    @property
    def log_filepath(self):
        pl = self.proc_list
        p = [i for i in pl if i.status in _active_status_codes]
        if not p:
            return None
        sel_proc = p[0]
        x = os.path.join(sel_proc.wdir, 'log')
        if os.path.exists(x):
            return x
        return ''

    @property
    def is_running(self):
        pl = self.proc_list
        return pl and any([i.status in _active_status_codes for i in pl])

    @property
    def is_ready_to_respond(self):
        if not self.is_running:
            return False
        pl = self.proc_list
        return all([_is_serving(proc) for proc in pl])

    @property
    def proc_list(self):
        if self._proc_list is None:
            with self._status_lock:
                if self._proc_list is None:
                    self._diagnose_status()
        return self._proc_list

    def kill(self):
        failed = False
        for proc in self.proc_list:
            if proc.status in [RStatus.RUNNING, RStatus.KILL_SENT]:
                assert isinstance(proc, Process)
                if kill_pid_or_false(proc.pid):
                    proc.status = RStatus.KILL_SENT
                    self._session.commit()
                    logger(__name__).info("Sent kill to {}".format(proc.pid))
                else:
                    logger(__name__).info("Could not kill {}".format(proc.pid))
                    failed = True
            else:
                m = "Skipping a non-running instance of {} in kill".format(self.service)
                logger(__name__).debug(m)
        return not failed

    def _diagnose_status(self):
        if self._session is None:
            self._session = get_new_session(None)
        if self.expected_pid:
            self._proc_list = [get_status_proc_for_pid(self._session, pid=self.expected_pid)[1]]
        else:
            self._proc_list = get_processdb_wrapper_for_active(self._session, name=self.service)

    def write_diagnosis(self, out=sys.stdout):
        """Returns the number of messages written (0 if the service does not occur in the checked
        history).
        """
        d = self._gen_diagnosis_message()
        if out is None:
            log = logger(__name__)
            for msg, level in d:
                log.log(level, msg)
        else:
            for p in d:
                out.write('{}\n'.format(p[0]))
        return len(d)

    def _gen_diagnosis_message(self):
        r = []
        for proc in self.proc_list:
            r.extend(self._gen_diagnosis_message_for_one(proc))
        return r

    @staticmethod
    def invocation(proc):
        wd = proc.archivedir if isinstance(proc, ArchivedProcess) else proc.wdir
        return os.path.join(wd, PROCESS_METADATA, 'invocation')

    @staticmethod
    def env_filename(proc):
        wd = proc.archivedir if isinstance(proc, ArchivedProcess) else proc.wdir
        return os.path.join(wd, PROCESS_METADATA, 'env')

    def _gen_diagnosis_message_for_one(self, proc):
        pstat = proc.status
        if pstat == RStatus.NOT_LAUNCHABLE:
            msg = '{} could not be launched. Perhaps the executable is not on the path. ' \
                  'Try:\n    {}\nand see the env in {}'
            msg = msg.format(self.service, self.invocation(proc), self.env_filename(proc))
            level = logging.ERROR
        elif pstat == RStatus.RUNNING:
            level = logging.INFO
            try:
                isready = _is_serving(proc)
            except:
                msg = '{} is running with PID={}'
            else:
                if isready:
                    msg = '{} is running with PID={} and ready to respond to requests.'
                else:
                    msg = '{} is running with PID={} but it is still booting.'
            msg = msg.format(self.service, proc.pid)
        else:
            if pstat == RStatus.ERROR_EXIT:
                level = logging.ERROR
            else:
                level = logging.WARN
            if isinstance(proc, ArchivedProcess):
                msg = _ARCHIVED_MESSAGES[pstat]
                msg = msg.format(s=self.service, a=proc.archivedir)
            else:
                msg = _NONARCHIVED_MESSAGES[pstat]
                msg = msg.format(s=self.service, w=proc.wdir)
        return [(msg, level)]


_ARCHIVED_MESSAGES = {
    RStatus.ERROR_EXIT: '{s} is exited with an error. Check "{a}"',
    RStatus.COMPLETED: '{s} completed and is no longer running. See "{a}"',
    RStatus.KILLED: '{s} was killed and is no longer running. See "{a}"',
    RStatus.ARCHIVED: '{s} is no longer running and has been archived at "{a}"',
}
_NONARCHIVED_MESSAGES = {
    RStatus.NOT_LAUNCHED: '{s} has not been launched by the job launcher',
    RStatus.ERROR_EXIT: '{s} is exited with an error. Check "{w}"',
    RStatus.COMPLETED: '{s} completed and is no longer running from "{w}"',
    RStatus.KILLED: '{s} was killed and is no longer running.',
    RStatus.KILL_SENT: 'A kill message has been sent to {s}, but it is still running',
    RStatus.BEING_ARCHIVED: '{s} is in the process of being archived',
    RStatus.MISSING_PRESUMED_ARCHIVED: '{s} is absent, it may have been archived.',
    RStatus.MISSING_NOT_ARCHIVED: '{s} is not present in the working space or archive',
}


def remove_archived(name):
    session = get_new_session()
    m = session.query(ArchivedProcess).filter_by(name=name).all()
    success = True
    for ap in m:
        ad = ap.archivedir
        if os.path.isdir(ad):
            logger(__name__).info('Removing archive of {} run from {}'.format(name, ad))
            success = remove_archived_job_artifacts(ad) and True
        session.delete(ap)
        session.commit()
    return success


def remove_archived_job_artifacts(tmp_dir_path):
    success = rm_files_and_dir_or_warn(os.path.join(tmp_dir_path, PROCESS_METADATA),
                                       ['env', 'stdoe', 'invocation'])
    success = rm_files_and_dir_or_warn(os.path.join(tmp_dir_path, 'logs'),
                                       ['myeasylog.log']) and success
    return rm_files_and_dir_or_warn(tmp_dir_path, ['log']) and success


def rm_files_and_dir_or_warn(par, filenames):
    success = True
    for fn in filenames:
        success = rm_or_warn(os.path.join(par, fn)) and success
    if success:
        return rmdir_or_warn(par)
    return success


def rmdir_or_warn(fp):
    try:
        if os.path.isdir(fp):
            os.rmdir(fp)
        return True
    except:
        logger(__name__).warn('rmdir of {} failed'.format(fp))
        return False


def rm_or_warn(fp):
    try:
        if os.path.isfile(fp):
            os.remove(fp)
        return True
    except:
        logger(__name__).warn('remove of {} failed'.format(fp))
        return False


def kill_pid_or_false(pid):
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
