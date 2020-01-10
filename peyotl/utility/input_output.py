#!/usr/bin/env python
"""Simple utility functions for Input/Output do not depend on any other part of
peyotl.
"""
from peyotl.utility.str_util import is_str_type, StringIO
from peyotl.utility.get_logger import get_logger
import shutil
import codecs
import json
import stat
import sys
import os
_LOG = get_logger(__name__)

def assure_dir_exists(d):
    if not os.path.exists(d):
        os.makedirs(d)

def shorter_fp_form(p):
    if os.path.isabs(p):
        ac = os.path.abspath(os.path.curdir)
        if p.startswith(ac):
            r = p[len(ac):]
            while r.startswith(os.sep):
                r = r[len(os.sep):]
            return r if len(r) < len(p) else p
        return p
    a = os.path.abspath(p)
    return a if len(a) < len(p) else p


def open_for_group_write(fp, mode, encoding='utf-8'):
    """Open with mode=mode and permissions '-rw-rw-r--' group writable is
    the default on some systems/accounts, but it is important that it be present on our deployment machine
    """
    d = os.path.split(fp)[0]
    assure_dir_exists(d)
    o = codecs.open(fp, mode, encoding=encoding)
    o.flush()
    os.chmod(fp, stat.S_IRGRP | stat.S_IROTH | stat.S_IRUSR | stat.S_IWGRP | stat.S_IWUSR)
    return o


def read_filepath(filepath, encoding='utf-8'):
    """Returns the text content of `filepath`"""
    with codecs.open(filepath, 'r', encoding=encoding) as fo:
        return fo.read()


def write_to_filepath(content, filepath, encoding='utf-8', mode='w', group_writeable=False):
    """Writes `content` to the `filepath` Creates parent directory
    if needed, and uses the specified file `mode` and data `encoding`.
    If `group_writeable` is True, the output file will have permissions to be
        writable by the group (on POSIX systems)
    """
    par_dir = os.path.split(filepath)[0]
    assure_dir_exists(par_dir)
    if group_writeable:
        with open_for_group_write(filepath, mode=mode, encoding=encoding) as fo:
            fo.write(content)
    else:
        with codecs.open(filepath, mode=mode, encoding=encoding) as fo:
            fo.write(content)


def expand_path(p):
    """Helper function to expand ~ and any environmental vars in a path string."""
    return os.path.expanduser(os.path.expandvars(p))


def download(url, encoding='utf-8'):
    """Returns the text fetched via http GET from URL, read as `encoding`"""
    import requests
    response = requests.get(url)
    response.encoding = encoding
    return response.text

def download_large_file(url, destination_filepath):
    """
    See http://stackoverflow.com/questions/16694907/how-to-download-large-file-in-python-with-requests-py
    by Roman Podlinov
    """
    import requests
    r = requests.get(url, stream=True)
    r.raise_for_status()
    par_dir = os.path.split(destination_filepath)[0]
    assure_dir_exists(par_dir)
    with open(destination_filepath, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
    return destination_filepath

def unzip(source, destination):
    import zipfile
    import shutil
    with zipfile.ZipFile(source, 'r') as z:
        z.extractall(destination)

def gunzip(source, destination):
    import gzip
    import shutil
    with gzip.open(source, 'rb') as f_in, open(destination, 'wb') as f_out:
        shutil.copyfileobj(f_in, f_out)


def gunzip_and_untar(source, destination, in_dir_mode=True):
    """If in_dir_mode is True, this function will put all of the contents of 
    the tarfile in destination, if they are not in top-level directory in the tar.
    If the tarfile contains one top level directory, then all of its elements will
    become children of `destination`. Essentially, this papers over whether or not
    the archive was created as a dir or set of files."""
    import tarfile
    import tempfile
    mode = 'r:gz' if sys.version_info.major else 'r|gz'
    t = tarfile.open(source, mode)
    to_safety_check = t.getnames()
    for n in to_safety_check:
        if n.startswith('..') or n.startswith('/') or n.startswith('~'):
            raise RuntimeError("untar failing because of dangerous element path: {}".format(n))
    td = tempfile.mkdtemp()
    dir_to_del = None
    try:
        t.extractall(td)
        assure_dir_exists(destination)
        ef = os.listdir(td)
        if len(ef) == 1 and os.path.isdir(os.path.join(td, ef[0])) and in_dir_mode:
            eff_par = os.path.join(td, ef[0])
            dir_to_del = eff_par
            to_move = os.listdir(eff_par)
        else:
            eff_par = td
            to_move = ef
        for n in to_move:
            src = os.path.join(eff_par, n)
            dest = os.path.join(destination, n)
            shutil.move(src, dest)
    finally:
        if dir_to_del:
            try:
                os.rmdir(dir_to_del)
            except OSError:
                _LOG.exception("Could not delete {}".format(os.path.abspath(dir_to_del)))
        try:
            os.rmdir(td)
        except OSError:
            _LOG.exception("Could not delete {}".format(os.path.abspath(td)))

def write_as_json(blob, dest, indent=0, sort_keys=True, separators=(', ', ': ')):
    """Writes `blob` as JSON to the filepath `dest` or the filestream `dest` (if it isn't a string)
    uses utf-8 encoding if the filepath is given (does not change the encoding if dest is already open).
    """
    opened_out = False
    if is_str_type(dest):
        out = codecs.open(dest, mode='w', encoding='utf-8')
        opened_out = True
    else:
        out = dest
    try:
        json.dump(blob, out, indent=indent, sort_keys=sort_keys, separators=separators)
        out.write('\n')
    finally:
        out.flush()
        if opened_out:
            out.close()


def pretty_dict_str(d, indent=2):
    """shows JSON indented representation of d"""
    b = StringIO()
    write_pretty_dict_str(b, d, indent=indent)
    return b.getvalue()


def write_pretty_dict_str(out, obj, indent=2):
    """writes JSON indented representation of `obj` to `out`"""
    json.dump(obj,
              out,
              indent=indent,
              sort_keys=True,
              separators=(',', ': '),
              ensure_ascii=False,
              encoding="utf-8")


def read_as_json(in_filename, encoding='utf-8'):
    with codecs.open(in_filename, 'r', encoding=encoding) as inpf:
        return json.load(inpf)


def parse_study_tree_list(fp):
    """study trees should be in {'study_id', 'tree_id'} objects, but
    as legacy support we also need to support files that have the format:
    pg_315_4246243 # comment

    """
    # noinspection PyBroadException
    try:
        sl = read_as_json(fp)
    except:
        sl = []
        with codecs.open(fp, 'rU', encoding='utf-8') as fo:
            for line in fo:
                frag = line.split('#')[0].strip()
                if frag:
                    sl.append(frag)
    ret = []
    for element in sl:
        if isinstance(element, dict):
            assert 'study_id' in element
            assert 'tree_id' in element
            ret.append(element)
        else:
            # noinspection PyUnresolvedReferences,PyUnresolvedReferences
            assert element.startswith('pg_') or element.startswith('ot_')
            # noinspection PyUnresolvedReferences
            s = element.split('_')
            assert len(s) > 1
            tree_id = s[-1]
            study_id = '_'.join(s[:-1])
            ret.append({'study_id': study_id, 'tree_id': tree_id})
    return ret
