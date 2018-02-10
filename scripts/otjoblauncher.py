#!/usr/bin/env python
"""Launches a job in the specified directory and uses the .process_metadata
subdirectory of that directory to record the status, pid, and returncode of the process

Invocation of this script:
    joblauncher.py <working dir> <path to stdin redirection or ''> <name_for_stdout> <name_for_stderr> <invocation word 1> <invocation word 2> ...

For example, the invocation:
    joblauncher.py wd in out err cmd arg1 arg2 arg3
would do the equivalent of  the shell script
################################################################################
cd wd
cmd arg1 arg2 arg3 < in >out 2>err
################################################################################


In addition to running the command and performing the redirection the script
creates the following directory structure (where wd is the working dir specified
as the first argument):

wd/.process_metadata
wd/.process_metadata/env            - python repr of os.environ
wd/.process_metadata/invocation     - the invocation used
wd/.process_metadata/launcher_pid   - pid of joblauncher.py instance
wd/.process_metadata/pid            - pid of launched process. This file will be
                                        absent if the launch fails (in this case
                                        the stderr file will contain a message
                                        from joblauncher.py).
wd/.process_metadata/stdioe         - A file with 3 lines. the file paths to
                                        stdin, stdout, and stderr (the
                                        joblauncher.py script's second through
                                        fourth arguments)
wd/.process_metadata/status         - will contain one of the following strings:
                                        "NOT_LAUNCHED",
                                        "NOT_LAUNCHABLE",
                                        "RUNNING",
                                        "ERROR_EXIT", or
                                        "COMPLETED"
If the process completes then:
wd/.process_metadata/returncode     - holds the returncode of the launched
                                        process. If the launching fails, then
                                        this file will hold -1

While the child process is running, any signal passed to this script will be passed along to the
    launched process.
"""
import signal
import subprocess
import codecs
import sys
import os

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


def open_metadata_file(d, fn, mode):
    fp = os.path.join(d, fn)
    return codecs.open(fp, mode, encoding='utf-8')


def flag_status(d, s):
    write_metadata(d, "status", '{}\n'.format(RStatus.to_str(s)))


def write_metadata(d, fn, content):
    with open_metadata_file(d, fn, 'w') as o:
        o.write(content)

launcher_err_stream = None
def log(msg):
    if launcher_err_stream:
        launcher_err_stream.write('{}\n'.format(msg))

def remove_metadata(d, fn):
    fp = os.path.join(d, fn)
    if os.path.exists(fp):
        try:
            os.remove(fp)
        except:
            log('Could not remove {}'.format(fp))

def shell_escape_arg(s):
    return "\\ ".join(s.split())


signal_handling_proc = None
kill_signal_sent = False


def pass_signal_to_proc(signum, stack_frame):
    global signal_handling_proc, kill_signal_sent
    p = signal_handling_proc
    log('launcher got signal {}'.format(signum))
    if p is not None:
        try:
            p.send_signal(signum)
        except Exception as x:
            log('Exception in signal {} handling: {}\n'.format(signum, str(x)))
        else:
            if signum == signal.SIGKILL:
                kill_signal_sent = True
                if metadata_dir:
                    flag_status(metadata_dir, RStatus.KILL_SENT)


metadata_dir = ''

def main():
    global signal_handling_proc, metadata_dir, launcher_err_stream
    all_signals = (signal.SIGABRT, signal.SIGALRM, signal.SIGFPE, signal.SIGILL, signal.SIGINT,
                   signal.SIGSEGV, signal.SIGTERM)
    if len(sys.argv) < 6 or (len(sys.argv) > 1 and sys.argv[1] == '-h'):
        sys.exit("""Expecting arguments the following arguments:
      path_to_parent_dir file_with_stdin name_for_stdout name_for_stderr
    followed by the command to invoke.
    """)
    wd = sys.argv[1]
    os.chdir(wd)
    stdinpath = sys.argv[2]
    stdoutpath = sys.argv[3]
    stderrpath = sys.argv[4]

    in_obj = stdinpath and open(stdinpath, 'rU') or None
    assert stdoutpath
    assert stderrpath
    outf = codecs.open(stdoutpath, "a", encoding='utf-8')
    if stderrpath == stdoutpath:
        errf = subprocess.STDOUT
        latererrf = outf
    else:
        errf = codecs.open(stderrpath, "a", encoding='utf-8')
        latererrf = errf
    invocation = sys.argv[5:]

    metadata_dir = ".process_metadata"
    if not os.path.exists(metadata_dir):
        os.mkdir(metadata_dir)

    lem = os.path.join(metadata_dir, 'launcher_err.txt')
    launcher_err_stream = codecs.open(lem, 'w', encoding='utf-8')

    escaped_invoc = ' '.join([shell_escape_arg(i) for i in invocation])
    write_metadata(metadata_dir, "invocation", "{i}\n".format(i=escaped_invoc))
    write_metadata(metadata_dir, "launcher_pid", "{e}\n".format(e=os.getpid()))
    try:
        try:
            with codecs.open(os.path.join(metadata_dir, "env"), "w", encoding='utf-8') as eout:
                for k, v in os.environ.items():
                    eout.write("export {}='{}'\n".format(k, "\'".join(v.split("'"))))
            write_metadata(metadata_dir, "stdioe", "{i}\n{o}\n{e}}\n".format(i=stdinpath,
                                                                             o=stdoutpath,
                                                                             e=stderrpath))
        except:
            pass
        flag_status(metadata_dir, RStatus.NOT_LAUNCHED)
        try:
            # print invocation
            proc = subprocess.Popen(invocation, stdin=subprocess.PIPE, stdout=outf, stderr=errf)
            flag_status(metadata_dir, RStatus.RUNNING)
        except:
            launcher_err_stream("Creation of subprocess failed\n")
            write_metadata(metadata_dir, "returncode", "-1\n")
            flag_status(metadata_dir, RStatus.NOT_LAUNCHABLE)
            return -1
        write_metadata(metadata_dir, "pid", "{p:d}\n".format(p=proc.pid))
        if in_obj:
            proc.stdin.write(in_obj.read())
        proc.stdin.close()
        # Register signal handlers to pass the signal to the launched process
        signal_handling_proc = proc
        for sig in all_signals:
            signal.signal(sig, pass_signal_to_proc)
        # wait for process exit
        proc.wait()
        rc = proc.returncode
        write_metadata(metadata_dir, "returncode", "{r:d}\n".format(r=rc))
        if kill_signal_sent:
            flag_status(metadata_dir, RStatus.KILLED)
        elif rc == 0:
            flag_status(metadata_dir, RStatus.COMPLETED)
        else:
            flag_status(metadata_dir, RStatus.ERROR_EXIT)
        # Deregister signal handlers
        signal_handling_proc = None
        for sig in all_signals:
            signal.signal(sig, signal.SIG_DFL)
        outf.close()
        if errf != subprocess.STDOUT:
            latererrf.close()
    finally:
        remove_metadata(metadata_dir, 'launcher_pid')
    return rc


if __name__ == '__main__':
    rc = 1
    try:
        rc = main()
    finally:
        if launcher_err_stream:
            launcher_err_stream.close()
    sys.exit(rc)
