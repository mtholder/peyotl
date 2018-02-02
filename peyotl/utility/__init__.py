#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Simple utility functions that do not depend on any other part of
peyotl.
"""
# Refactored based on advice in
#    https://fangpenlin.com/posts/2012/08/26/good-logging-practice-in-python/
from __future__ import print_function, division
# noinspection PyPep8Naming
from logging import getLogger as logger
import logging.config
import threading
import logging
import codecs
import json
import stat
import sys
import os
import re

_OPENTREE_CONFIG_DIR = None
_OPENTREE_CONFIG_DIR_IS_SET = False


def opentree_config_dir(config_dirpath=None):
    """Returns filepath to parent of config files and unlogged messages.

    The cascade is to return:
        1. (highest) config_dirpath
        2. path in env OPENTREE_CONFIG_DIR variable
        3. ~/.opentreeoflife
    Will return the filepath (or None if not an existing dir) and a list of (level, message) pairs
        to be logged. (Since this is called in logger setup, it does not log events).
    This call will only have an effect if called before any use of `logger`, as the first
        call's filepath is cached and returned in all subsequent invocations.
    """
    global _OPENTREE_CONFIG_DIR, _OPENTREE_CONFIG_DIR_IS_SET
    if _OPENTREE_CONFIG_DIR_IS_SET:
        return _OPENTREE_CONFIG_DIR, []
    config_dir_env_key = 'OPENTREE_CONFIG_DIR'
    queued_messages = []
    fp = os.environ.get(config_dir_env_key) if config_dirpath is None else config_dirpath
    if fp is not None:
        if config_dirpath is None:
            src = 'the {} env var'.format(config_dir_env_key)
        else:
            src = 'function argument'
        if not os.path.isdir(fp):
            m = '"{}" (obtained from {}) is not a directory.'
            queued_messages.append((logging.WARN, m.format(fp, src)))
            fp = None
        else:
            m = 'Directory "{}" (obtained from {}) used as parent of config files.'
            queued_messages.append((logging.DEBUG, m.format(fp, src)))
    else:
        fp = os.path.expanduser('~/.opentreeoflife')
        m = 'Using default "{}" as parent directory of config files.'
        queued_messages.append((logging.DEBUG, m.format(fp)))
        if not os.path.isdir(fp):
            m = 'Directory "{}" does not exist. No peyotl configuration will be read'
            queued_messages.append((logging.WARN, m.format(fp, config_dir_env_key)))
            fp = None
    _OPENTREE_CONFIG_DIR_IS_SET = True,
    _OPENTREE_CONFIG_DIR = fp
    return _OPENTREE_CONFIG_DIR, queued_messages


def _get_default_peyotl_log_ini_filepath(config_dirpath=None):
    r, queued = opentree_config_dir(config_dirpath=config_dirpath)
    if r:
        r = os.path.join(r, 'peyotl_logging.ini')
    return r, queued


def configure_logger(cfg_filepath=None,
                     env_key='PEYOTL_LOG_INI_FILEPATH',
                     config_dirpath=None,
                     default_level=logging.INFO):
    queued = []
    if cfg_filepath is None:
        fp = os.environ.get(env_key)
        from_env = fp is not None
        if fp is None:
            fp, queued = _get_default_peyotl_log_ini_filepath(config_dirpath=config_dirpath)
        if from_env:
            m = 'logging INI filepath from {} env var.'.format(env_key)
        else:
            m = 'logging INI filepath from default peyotl log INI cascade.'
        queued.append((logging.DEBUG, m))
        fp, q = _read_configure_logging_from_fp(fp, default_level=default_level)
        queued.extend(q)
    else:
        fp, queued = _read_configure_logging_from_fp(filepath=cfg_filepath,
                                                     default_level=default_level)
    log = logger(__name__)
    for level, msg in queued:
        log.log(level=level, msg=msg)
    return fp


def _read_configure_logging_from_fp(filepath, default_level=logging.INFO):
    queued = []
    ready = False
    if filepath and os.path.isfile(filepath):
        try:
            logging.config.fileConfig(filepath, disable_existing_loggers=False)
            queued.append((logging.DEBUG, 'logging configured based on "{}"'.format(filepath)))
            ready = True
        except Exception as x:
            m = 'Exception when reading "{}" : {}'.format(filepath, str(x))
            queued.append((logging.ERROR, m))
    if not ready:
        logging.basicConfig(level=default_level)
        m = 'logging defaulting to level={} because "{}" does not exist'
        queued.append((logging.DEBUG, m.format(default_level, filepath)))
        filepath = None
    return filepath, queued


####################################################################################################
# Str-util
if sys.version_info.major == 2:
    # noinspection PyCompatibility
    from cStringIO import StringIO
    from io import BytesIO

    UNICODE = unicode


    def is_str_type(x):
        # noinspection PyCompatibility
        return isinstance(x, basestring)


    def is_int_type(x):
        return isinstance(x, int) or isinstance(x, long)


    def get_utf_8_string_io_writer():
        string_io = BytesIO()
        wrapper = codecs.getwriter("utf8")(string_io)
        return string_io, wrapper


    def flush_utf_8_writer(wrapper):
        wrapper.reset()


    def get_utf_8_value(s):
        return s.getvalue().decode('utf-8')


    def reverse_dict(d):
        """returns a v->k dict. Behavior undefined if values are not unique"""
        # noinspection PyCompatibility
        return {v: k for k, v in d.iteritems()}
else:
    from io import StringIO  # pylint: disable=E0611,W0403

    UNICODE = str


    def is_str_type(x):
        return isinstance(x, str)


    def is_int_type(x):
        return isinstance(x, int)


    def get_utf_8_string_io_writer():
        string_io = StringIO()
        return string_io, string_io


    # noinspection PyUnusedLocal
    def flush_utf_8_writer(wrapper):
        pass


    def get_utf_8_value(s):
        return s.getvalue()


    def reverse_dict(d):
        """returns a v->k dict. Behavior undefined if values are not unique"""
        return {v: k for k, v in d.items()}


def slugify(s):
    """Convert any string to a "slug", a simplified form suitable for filename and URL part.
     EXAMPLE: "Trees about bees" => 'trees-about-bees'
     EXAMPLE: "My favorites!" => 'my-favorites'
    N.B. that its behavior should match this client-side slugify function, so
    we can accurately "preview" slugs in the browser:
     https://github.com/OpenTreeOfLife/opentree/blob/553546942388d78545cc8dcc4f84db78a2dd79ac/curator/static/js/curation-helpers.js#L391-L397
    TODO: Should we also trim leading and trailing spaces (or dashes in the final slug)?
    """
    slug = s.lower()  # force to lower case
    slug = re.sub('[^a-z0-9 -]', '', slug)  # remove invalid chars
    slug = re.sub(r'\s+', '-', slug)  # collapse whitespace and replace by -
    slug = re.sub('-+', '-', slug)  # collapse dashes
    if not slug:
        slug = 'untitled'
    return slug


def increment_slug(s):
    """Generate next slug for a series.

       Some docstore types will use slugs (see above) as document ids. To
       support unique ids, we'll serialize them as follows:
         TestUserA/my-test
         TestUserA/my-test-2
         TestUserA/my-test-3
         ...
    """
    slug_parts = s.split('-')
    # advance (or add) the serial counter on the end of this slug
    # noinspection PyBroadException
    try:
        # if it's an integer, increment it
        slug_parts[-1] = str(1 + int(slug_parts[-1]))
    except:
        # there's no counter! add one now
        slug_parts.append('2')
    return '-'.join(slug_parts)


def underscored2camel_case(v):
    """converts ott_id to ottId.

    Capitalizes after the _ but does not change other chars to lower case"""
    vlist = v.split('_')
    c = []
    for n, el in enumerate(vlist):
        if el:
            if n == 0:
                c.append(el)
            else:
                c.extend([el[0].upper(), el[1:]])
    return ''.join(c)


# End str-util
####################################################################################################
# Input-output

def assure_dir_exists(d):
    """Creates directory `d` if not existing or raises an exception"""
    if not os.path.exists(d):
        os.makedirs(d)


def shorter_fp_form(p):
    """Returns the shortest string representation of a path. Could be relative to $PWD or abs."""
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
    the default on some systems/accounts, but it is important that it be present on our deployment
    machine
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
    """See
http://stackoverflow.com/questions/16694907/how-to-download-large-file-in-python-with-requests-py
    by Roman Podlinov
    """
    import requests
    r = requests.get(url, stream=True)
    par_dir = os.path.split(destination_filepath)[0]
    assure_dir_exists(par_dir)
    with open(destination_filepath, 'wb') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                f.write(chunk)
    return destination_filepath


def unzip(source, destination):
    import zipfile
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
            os.rename(src, dest)
    finally:
        if dir_to_del:
            try:
                os.rmdir(dir_to_del)
            except OSError:
                m = "Could not delete {}".format(os.path.abspath(dir_to_del))
                logger(__name__).exception(m)
        try:
            os.rmdir(td)
        except OSError:
            logger(__name__).exception("Could not delete {}".format(os.path.abspath(td)))


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
    b, w = get_utf_8_string_io_writer()
    write_pretty_dict_str(w, d, indent=indent)
    flush_utf_8_writer(w)
    return get_utf_8_value(b)


def write_pretty_dict_str(out, obj, indent=2):
    """writes JSON indented representation of `obj` to `out`"""
    kwargs = {}
    if sys.version_info.major == 2:
        kwargs['encoding'] = 'utf-8'
    json.dump(obj,
              out,
              indent=indent,
              sort_keys=True,
              separators=(',', ': '),
              ensure_ascii=False,
              **kwargs)


def read_as_json(in_filename, encoding='utf-8'):
    with codecs.open(in_filename, 'r', encoding=encoding) as inpf:
        return json.load(inpf)


# End Input-output
####################################################################################################


# !/usr/bin/env python
# -*- coding: utf-8 -*-
"""Functions for interacting with a runtime-configuration.
Typical usage:
    setting = get_config_setting('phylesystem', 'parent', default=None)
Or:
    cfg = get_config_object()
    setting = cfg.get_config_setting('phylesystem', 'parent', default=None)
The latter is more efficient, if you have several settings to check. The former is just sugar for the latter.

The returned `cfg` from get_config_object is a singleton, immutable ConfigWrapper instance. It is populated based
on peyotl's system of finding configuration file locations (see below)
Returned settings are always strings (no type coercion is supported)


The ConfigWrapper class also has a
    setting = cfg.get_from_config_setting_cascade([('apis', 'oti_raw_urls'), ('apis', 'raw_urls')], "FALSE")
syntax to allow client code to check one section+param name after another


It is possible to override settings by creating your own ConfigWrapper instance and using the overrides argument to the
__init__ call, but that is mainly useful for Mock configurations used in testing.

The function `read_config` is an alias for `get_raw_default_config_and_read_file_list` both should be treated as
deprecated.

# Peyotl config file finding/ Env variables.
The default peyotl configuration is read from a file. The configuration filepath is either the value of
    the PEYOTL_CONFIG_FILE environmental variable (if that variable is defined) or
    ~/.opentreeoflife/peyotl.ini
If that filepath does not exist, then the fallback is the config that is the `default_peyotl.ini`
    file that is packaged with peyotl.
get_raw_default_config_and_read_file_list() is the accessor for the raw ConfigParser object and list of files
    that were used to populate that file.

If you want to check your configuration, you can run:

    python scripts/clipeyotl.py config -a list

to see the union of all your settings along with comments describing where those settings come from (e.g. which
are from your environment).

"""

_CONFIG = None
_CONFIG_FN = None
_READ_DEFAULT_FILES = None
_DEFAULT_CONFIG_WRAPPER = None
_DEFAULT_CONFIG_WRAPPER_LOCK = threading.Lock()
_CONFIG_FN_LOCK = threading.Lock()
_CONFIG_LOCK = threading.Lock()


def _replace_default_config(cfg):
    """Only to be used internally for testing !
    """
    global _CONFIG
    _CONFIG = cfg


def get_default_config_filename():
    """Returns the configuration filepath.

    If PEYOTL_CONFIG_FILE is in the env that is the preferred choice; otherwise ~/.peyotl/config is preferred.
    If the preferred file does not exist, then the packaged peyotl/default.conf from the installation of peyotl is
    used.
    A RuntimeError is raised if that fails.
    """
    global _CONFIG_FN
    if _CONFIG_FN is not None:
        return _CONFIG_FN
    with _CONFIG_FN_LOCK:
        if _CONFIG_FN is not None:
            return _CONFIG_FN
        env_value = os.environ.get('PEYOTL_CONFIG_FILE')
        dfn = 'peyotl.ini'
        cfn = env_value if env_value is not None else os.path.join(opentree_config_dir(), dfn)
        cfn = os.path.abspath(cfn)
        if not os.path.isfile(cfn):
            # noinspection PyProtectedMember
            if env_value is not None:
                msg = 'Filepath "{}" specified via PEYOTL_CONFIG_FILE={} was not found'
                logger(__name__).warn(msg.format(cfn, env_value))
            from pkg_resources import Requirement, resource_filename
            pr = Requirement.parse('peyotl')
            cfn = resource_filename(pr, 'peyotl/default_peyotl.ini')
        if not os.path.isfile(cfn):
            msg = 'The peyotl configuration file cascade failed looking for "{}"'.format(cfn)
            logger(__name__).warn(msg)
            _CONFIG_FN = ''
        else:
            _CONFIG_FN = os.path.abspath(cfn)
    return _CONFIG_FN


def get_raw_default_config_and_read_file_list():
    """Returns a ConfigParser object and a list of filenames that were parsed to initialize it"""
    global _CONFIG, _READ_DEFAULT_FILES
    if _CONFIG is not None:
        return _CONFIG, _READ_DEFAULT_FILES
    with _CONFIG_LOCK:
        if _CONFIG is not None:
            return _CONFIG, _READ_DEFAULT_FILES
        try:
            # noinspection PyCompatibility
            from ConfigParser import SafeConfigParser
        except ImportError:
            # noinspection PyCompatibility,PyUnresolvedReferences
            from configparser import ConfigParser as SafeConfigParser  # pylint: disable=F0401
        cfg = SafeConfigParser()
        cfn = get_default_config_filename()
        read_files = cfg.read(cfn) if cfn else []
        _CONFIG, _READ_DEFAULT_FILES = cfg, read_files
        return _CONFIG, _READ_DEFAULT_FILES


# Alias the correct function name to our old alias
# # @TODO Deprecate when phylesystem-api calls the right function
read_config = get_raw_default_config_and_read_file_list


def _warn_missing_setting(section, param, config_filename, warn_on_none_level=logging.WARN):
    if warn_on_none_level is None:
        return
    if config_filename:
        if not is_str_type(config_filename):
            f = ' "{}" '.format('", "'.join(config_filename))
        else:
            f = ' "{}" '.format(config_filename)
    else:
        f = ' '
    mf = 'Config file {f} does not contain option "{o}"" in section "{s}"'
    msg = mf.format(f=f, o=param, s=section)
    logger(__name__).warn(msg)


def _raw_config_setting(config_obj, section, param, default=None, config_filename='',
                        warn_on_none_level=logging.WARN):
    """Read (section, param) from `config_obj`. If not found, return `default`

    If the setting is not found and `default` is None, then an warn-level message is logged.
    `config_filename` can be None, filepath or list of filepaths - it is only used for logging.

    If warn_on_none_level is None (or lower than the logging level) message for falling through to
        a `None` default will be suppressed.
    """
    # noinspection PyBroadException
    try:
        return config_obj.get(section, param)
    except:
        if (default is None) and warn_on_none_level is not None:
            _warn_missing_setting(section, param, config_filename, warn_on_none_level)
        return default


class ConfigWrapper(object):
    """Wrapper to provide a richer get_config_setting(section, param, d) behavior. That function will
    return a value from the following cascade:
        1. override[section][param] if section and param are in the resp. dicts
        2. the value in the config obj for section/param if that setting is in the config
    Must be treated as immutable!
    """

    def __init__(self,
                 raw_config_obj=None,
                 config_filename='<programmatically configured>',
                 overrides=None):
        """If `raw_config_obj` is None, the default peyotl cascade for finding a configuration
        file will be used. overrides is a 2-level dictionary of section/param entries that will
        be used instead of the setting.
        The two default dicts will be used if the setting is not in overrides or the config object.
        "dominant" and "fallback" refer to whether the rank higher or lower than the default
        value in a get.*setting.*() call
        """
        # noinspection PyProtectedMember
        self._config_filename = config_filename
        self._raw = raw_config_obj
        if overrides is None:
            overrides = {}
        self._override = overrides

    @property
    def config_filename(self):
        self._assure_raw()
        return self._config_filename

    @property
    def raw(self):
        self._assure_raw()
        return self._raw

    def get_from_config_setting_cascade(self, sec_param_list, default=None,
                                        warn_on_none_level=logging.WARN):
        """return the first non-None setting from a series where each
        element in `sec_param_list` is a section, param pair suitable for
        a get_config_setting call.

        Note that non-None values for overrides for this ConfigWrapper instance will cause
            this call to only evaluate the first element in the cascade.
        """
        for section, param in sec_param_list:
            r = self.get_config_setting(section, param, default=None, warn_on_none_level=None)
            if r is not None:
                return r
        section, param = sec_param_list[-1]
        if default is None:
            _warn_missing_setting(section, param, self._config_filename, warn_on_none_level)
        return default

    def _assure_raw(self):
        if self._raw is None:
            cfg, fllist = get_raw_default_config_and_read_file_list()
            self._raw = cfg
            assert len(fllist) == 1
            self._config_filename = fllist[0]

    def get_config_setting(self, section, param, default=None, warn_on_none_level=logging.WARN):
        if section in self._override:
            so = self._override[section]
            if param in so:
                return so[param]
        self._assure_raw()
        return _raw_config_setting(self._raw,
                                   section,
                                   param,
                                   default=default,
                                   config_filename=self._config_filename,
                                   warn_on_none_level=warn_on_none_level)

    def report(self, out):
        # noinspection PyProtectedMember
        cfn = self.config_filename
        out.write('# Config read from "{f}"\n'.format(f=cfn))
        cfenv = os.environ.get('PEYOTL_CONFIG_FILE', '')
        if cfenv:
            if os.path.abspath(cfenv) == cfn:
                emsg = '#  config filepath obtained from $PEYOTL_CONFIG_FILE env var.\n'
            else:
                emsg = '#  using packaged default. The filepath from PEYOTL_CONFIG_FILE env. var. did not exist.\n'
        else:
            cfhome = os.path.expanduser("~/.peyotl/config")
            if os.path.abspath(cfhome) == cfn:
                emsg = "#  config filepath obtained via ~/.peyotl/config convention.\n"
            else:
                emsg = '#  using packaged default. PEYOTL_CONFIG_FILE was not set and ~/.peyotl/config was not found.\n'
        out.write(emsg)
        from_raw = _create_overrides_from_config(self._raw)
        k = set(self._override.keys())
        k.update(from_raw.keys())
        k = list(k)
        k.sort()
        for key in k:
            ov_set = self._override.get(key, {})
            fr_set = from_raw.get(key, {})
            v = set(ov_set.keys())
            v.update(fr_set.keys())
            v = list(v)
            v.sort()
            out.write('[{s}]\n'.format(s=key))
            for param in v:
                if param in ov_set:
                    out.write('# {p} from override\n{p} = {s}\n'.format(p=param,
                                                                        s=str(ov_set[param])))
                else:
                    out.write('# {p} from {f}\n{p} = {s}\n'.format(p=param,
                                                                   s=str(fr_set[param]),
                                                                   f=self._config_filename))


def _create_overrides_from_config(config):
    """Creates a two-level dictionary of d[section][option] from a config parser object."""
    d = {}
    for s in config.sections():
        d[s] = {}
        for opt in config.options(s):
            d[s][opt] = config.get(s, opt)
    return d


def get_config_setting(section, param, default=None):
    """
    """
    cfg = get_config_object()
    return cfg.get_config_setting(section, param, default=default)


def get_config_object():
    """Thread-safe accessor for the immutable default ConfigWrapper object"""
    global _DEFAULT_CONFIG_WRAPPER
    if _DEFAULT_CONFIG_WRAPPER is not None:
        return _DEFAULT_CONFIG_WRAPPER
    with _DEFAULT_CONFIG_WRAPPER_LOCK:
        if _DEFAULT_CONFIG_WRAPPER is not None:
            return _DEFAULT_CONFIG_WRAPPER
        _DEFAULT_CONFIG_WRAPPER = ConfigWrapper()
        return _DEFAULT_CONFIG_WRAPPER


# end Config
####################################################################################################

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


'''
from peyotl.utility.input_output import (assure_dir_exists,
                                         download,
                                         download_large_file,
                                         expand_path,
                                         open_for_group_write,
                                         parse_study_tree_list,
                                         write_to_filepath)

import peyotl.utility.get_logger
from peyotl.utility.get_logger import logger
from peyotl.utility.get_config import (ConfigWrapper, get_config_setting, get_config_object, read_config,
                                       get_raw_default_config_and_read_file_list)
import time
import os

__all__ = ['input_output', 'simple_file_lock', 'str_util', 'logger', 'dict_wrapper', 'tokenizer', 'get_config']

def add_or_append_to_dict(d, k, v):
    """If dict `d` has key `k`, then the new value of that key will be appended
    onto a list containing the previous values, otherwise d[k] = v.
    Creates a lightweight multimap, but not safe if v can be None or a list.
    returns True if the k now maps to >1 value"""
    ov = d.get(k)
    if ov is None:
        d[k] = v
        return False
    if isinstance(ov, list):
        ov.append(v)
    else:
        d[k] = [ov, v]
    return True

def any_early_exit(iterable, predicate):
    """Tests each element in iterable by calling predicate(element). Returns True on first True, or False."""
    for i in iterable:
        if predicate(i):
            return True
    return False


def pretty_timestamp(t=None, style=0):
    if t is None:
        t = time.localtime()
    if style == 0:
        return time.strftime("%Y-%m-%d", t)
    return time.strftime("%Y%m%d%H%M%S", t)


def doi2url(v):
    if v.startswith('http'):
        return v
    if v.startswith('doi:'):
        if v.startswith('doi: '):
            v = v[5:]  # trim 'doi: '
        else:
            v = v[4:]  # trim 'doi:'
    if v.startswith('10.'):  # it's a DOI!
        return 'http://dx.doi.org/' + v
    # convert anything else to URL and hope for the best
    return 'http://' + v


def get_unique_filepath(stem):
    """NOT thread-safe!
    return stems or stem# where # is the smallest
    positive integer for which the path does not exist.
    useful for temp dirs where the client code wants an
    obvious ordering.
    """
    fp = stem
    if os.path.exists(stem):
        n = 1
        fp = stem + str(n)
        while os.path.exists(fp):
            n += 1
            fp = stem + str(n)
    return fp


def propinquity_fn_to_study_tree(inp_fn, strip_extension=True):
    """This should only be called by propinquity - other code should be treating theses
    filenames (and the keys that are based on them) as opaque strings.

    Takes a filename (or key if strip_extension is False), returns (study_id, tree_id)

    propinquity provides a map to look up the study ID and tree ID (and git SHA)
    from these strings.
    """
    if strip_extension:
        study_tree = '.'.join(inp_fn.split('.')[:-1])  # strip extension
    else:
        study_tree = inp_fn
    x = study_tree.split('@')
    if len(x) != 2:
        msg = 'Currently we are expecting studyID@treeID.<file extension> format. Expected exactly 1 @ in the filename. Got "{}"'
        msg = msg.format(study_tree)
        raise ValueError(msg)
    return x
'''
