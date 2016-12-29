"""Base class for individual shard (repo) used in a doc store.
   Subclasses will accommodate each type."""
import re
import os
import codecs
import anyjson
from threading import Lock
from peyotl.utility import get_logger, write_to_filepath
from peyotl.utility.input_output import read_as_json, write_as_json
from peyotl.git_storage.git_action import GitActionBase

_LOG = get_logger(__name__)


class FailedShardCreationError(ValueError):
    def __init__(self, message):
        ValueError.__init__(self, message)


class GitShard(object):
    """Bare-bones functionality needed by both normal and proxy shards."""

    def __init__(self, name, document_schema=None, path_mapper=None):
        self._index_lock = Lock()
        self._doc_index = {}
        self.name = name
        self.path = ' '
        # ' ' mimics place of the abspath of repo in path -> relpath mapping
        self.has_aliases = False
        self._new_doc_prefix = None
        self.document_schema = document_schema
        self.path_mapper = path_mapper


    # pylint: disable=E1101
    def get_rel_path_fragment(self, doc_id):
        """For `doc_id` returns the path from the
        repo to the doc file. This is useful because
        (if you know the remote), it lets you construct the full path.
        """
        with self._index_lock:
            r = self._doc_index[doc_id]
        fp = r[-1]
        return fp[(len(self.path) + 1):]  # "+ 1" to remove the /

    def validate_annotate_convert_doc(self, document, **kwargs):
        return self.document_schema.validate_annotate_convert_doc(document, **kwargs)

    @property
    def document_type(self):
        return self.document_schema.document_type

    @property
    def doc_index(self):
        return self._doc_index

    @doc_index.setter
    def doc_index(self, val):
        self._doc_index = val

    def get_doc_ids(self, **kwargs):
        with self._index_lock:
            k = self._doc_index.keys()
        return list(k)

    @property
    def new_doc_prefix(self):
        return self._new_doc_prefix

class GitShardProxy(GitShard):
    def __init__(self, config, config_key, path_mapper, document_schema):
        if len(path_mapper.doc_holder_subpath_list) != 1:
            raise NotImplementedError("Cannot create a GitShardProxy around a doc store with" \
                                      "multiple doctypes")
        GitShard.__init__(self,
                          config['name'],
                          path_mapper=path_mapper,
                          document_schema=document_schema)
        dhp = path_mapper.doc_holder_subpath_list[0]
        d = {}
        for obj in config[config_key]:
            kl = obj['keys']
            if len(kl) > 1:
                _LOG.warn("aliases not supported in shards")
            for k in kl:
                complete_path = '{p}/{s}/{r}'.format(p=self.path,
                                                     s=dhp,
                                                     r=obj['relpath'])
                d[k] = (self.name, self.path, complete_path)
        self.doc_index = d


class TypeAwareGitShard(GitShard):
    """Adds hooks for type-specific behavior in subclasses.
    """

    def __init__(self,
                 name,
                 path,
                 document_schema,
                 push_mirror_repo_path=None,
                 infrastructure_commit_author='OpenTree API <api@opentreeoflife.org>',
                 max_file_size=None,
                 path_mapper=None):
        GitShard.__init__(self, name, document_schema=document_schema, path_mapper=path_mapper)
        self._doc_counter_lock = Lock()
        self._infrastructure_commit_author = infrastructure_commit_author
        self._master_branch_repo_lock = Lock()
        path = os.path.abspath(path)
        if not os.path.isdir(path):
            raise FailedShardCreationError('"{p}" is not a directory'.format(p=path))
        dot_git = os.path.join(path, '.git')
        if not os.path.isdir(dot_git):
            raise FailedShardCreationError('"{p}" is not a directory'.format(p=dot_git))
        for dhp in path_mapper.doc_holder_subpath_list:  # dhp is type-specific, e.g. 'study'
            doc_dir = os.path.join(path, dhp)
            if not os.path.isdir(doc_dir):
                raise FailedShardCreationError('"{p}" is not a directory'.format(p=doc_dir))
        self.path = path
        self.doc_dir = doc_dir
        with self._index_lock:
            self._locked_refresh_doc_index()
        self.parent_path = os.path.split(path)[0] + '/'
        self.git_dir = dot_git
        self.push_mirror_repo_path = push_mirror_repo_path
        if max_file_size is not None:
            try:
                max_file_size = int(max_file_size)
            except:
                m = 'Configuration-base value of max_file_size was "{}". Expecting an integer.'
                m = m.format(max_file_size)
                raise RuntimeError(m)
        self.max_file_size = max_file_size
        self._known_prefixes = None
        for prefix_filename in ['new_study_prefix', 'new_doc_prefix']:
            prefix_filepath = os.path.join(path, prefix_filename)
            if os.path.exists(prefix_filepath):
                with open(prefix_filepath, 'r') as f:
                    pre_content = f.read().strip()
                valid_pat = re.compile('^[a-zA-Z0-9]+_$')
                if len(pre_content) != 3 or not valid_pat.match(pre_content):
                    raise FailedShardCreationError('Expecting prefix in {} file to be two '
                                                   'letters followed by an underscore'.format(prefix_filename))
                self._new_doc_prefix = pre_content

    def _locked_create_id2document_info(self):
        """Searchers for JSON files in this repo and returns
        a map of collection id ==> (`tag`, dir, collection filepath)
        where `tag` is typically the shard name
        """
        d = {}
        dd = self.path
        if not dd.endswith(os.path.sep):
            dd = dd + os.path.sep
        ldd = len(dd)
        for triple in os.walk(self.doc_dir):
            root, files = triple[0], triple[2]
            for filename in files:
                if filename.endswith('.json'):
                    full_path = os.path.join(root, filename)
                    assert full_path.startswith(dd)
                    rel_path = full_path[ldd:]
                    doc_id = self.path_mapper.id_from_rel_path(rel_path)
                    if doc_id is not None:
                        d[doc_id] = (self.name, root, os.path.join(root, full_path))
        return d

    def _locked_refresh_doc_index(self):
        d = self._locked_create_id2document_info()
        self._doc_index = d

    @property
    def known_prefixes(self):
        if self._known_prefixes is None:
            self._known_prefixes = self._diagnose_prefixes()
        return self._known_prefixes

    def _diagnose_prefixes(self):
        raise NotImplementedError("_diagnose_prefixes must be overridden.")

    def can_mint_new_docs(self):
        return True  # phylesystem shards can only mint new IDs if they have a new_doc_prefix file, overridden.

    def delete_doc_from_index(self, doc_id):
        with self._index_lock:
            try:
                del self._doc_index[doc_id]
            except:
                pass

    def create_git_action(self):
        return GitActionBase(doc_type=self.document_type,
                             repo=self.path,
                             max_file_size=self.max_file_size,
                             path_mapper=self.path_mapper)

    def create_git_action_for_mirror(self):
        return GitActionBase(doc_type=self.document_type,
                             repo=self.push_mirror_repo_path,
                             max_file_size=self.max_file_size,
                             path_mapper=self.path_mapper)

    def pull(self, remote='origin', branch_name='master'):
        with self._index_lock:
            ga = self.create_git_action()
            from peyotl.git_storage.git_workflow import _pull_gh
            _pull_gh(ga, remote, branch_name)
            self._locked_refresh_doc_index()

    def register_doc_id(self, ga, doc_id):
        fp = ga.path_for_doc(doc_id)
        with self._index_lock:
            self._doc_index[doc_id] = (self.name, self.doc_dir, fp)


    def push_to_remote(self, remote_name):
        if self.push_mirror_repo_path is None:
            raise RuntimeError('This {} has no push mirror, so it cannot push to a remote.'.format(type(self)))
        working_ga = self.create_git_action()
        mirror_ga = self.create_git_action_for_mirror()
        with mirror_ga.lock():
            with working_ga.lock():
                mirror_ga.fetch(remote='origin')
            mirror_ga.merge('origin/master', destination='master')
            mirror_ga.push(branch='master',
                           remote=remote_name)
        return True

    def iter_doc_filepaths(self, **kwargs):  # pylint: disable=W0613
        """Returns a pair: (doc_id, absolute filepath of document file)
        for each document in this repository.
        Order is arbitrary.
        """
        with self._index_lock:
            for doc_id, info in self._doc_index.items():
                yield doc_id, info[-1]

    # TODO:type-specific? Where and how is this used?
    def iter_doc_objs(self, **kwargs):
        """Returns a pair: (doc_id, nexson_blob)
        for each document in this repository.
        Order is arbitrary.
        """
        try:
            for doc_id, fp in self.iter_doc_filepaths(**kwargs):
                # TODO:hook for type-specific parser?
                with codecs.open(fp, 'r', 'utf-8') as fo:
                    try:
                        nex_obj = anyjson.loads(fo.read())
                        yield (doc_id, nex_obj)
                    except Exception:
                        _LOG.warn('Loading of doc {} from {} failed in iter_doc_obj'.format(doc_id, fp))
                        pass
        except Exception as x:
            f = 'iter_doc_filepaths FAILED with this error:\n{}'
            f = f.format(str(x))
            _LOG.warn(f)

    def write_configuration(self, out, secret_attrs=False):
        """Generic configuration, may be overridden by type-specific version"""
        cd = self.get_configuration_dict(secret_attrs=secret_attrs)
        key_order = cd.keys()
        key_order.sort()
        for k in key_order:
            out.write('  {} = {}'.format(k, cd[k]))
        for o in cd['documents']:
            out.write('    {} ==> {}\n'.format(o['keys'], o['relpath']))

    def get_configuration_dict(self, secret_attrs=False):
        """Generic configuration, may be overridden by type-specific version"""
        rd = {'name': self.name,
              'path': self.path,
              'document_schema': repr(self.document_schema)
              }
        with self._index_lock:
            si = self._doc_index
        r = _invert_dict_list_val(si)
        key_list = list(r.keys())
        rd['number of documents'] = len(key_list)
        key_list.sort()
        m = []
        for k in key_list:
            v = r[k]
            fp = k[2]
            assert fp.startswith(self.doc_dir)
            rp = fp[len(self.doc_dir) + 1:]
            m.append({'keys': v, 'relpath': rp})
        rd['documents'] = m
        return rd

    def get_branch_list(self):
        ga = self.create_git_action()
        return ga.get_branch_list()

    def get_changed_docs(self, ancestral_commit_sha, doc_ids_to_check=None):
        ga = self.create_git_action()
        return ga.get_changed_docs(ancestral_commit_sha, doc_ids_to_check=doc_ids_to_check)
        # TODO:git-action-edits

    def _read_master_branch_resource(self, fn, is_json=False):
        """This will force the current branch to master! """
        with self._master_branch_repo_lock:
            ga = self.create_git_action()
            with ga.lock():
                ga.checkout_master()
                if os.path.exists(fn):
                    if is_json:
                        return read_as_json(fn)
                    with codecs.open(fn, 'rU', encoding='utf-8') as f:
                        ret = f.read()
                    return ret
                return None

    def _write_master_branch_resource(self, content, fn, commit_msg, is_json=False):
        """This will force the current branch to master! """
        # TODO: we might want this to push, but currently it is only called in contexts in which
        # we are about to push anyway (document creation)
        with self._master_branch_repo_lock:
            ga = self.create_git_action()
            with ga.lock():
                ga.checkout_master()
                if is_json:
                    write_as_json(content, fn)
                else:
                    write_to_filepath(content, fn)
                ga._add_and_commit(fn, self._infrastructure_commit_author, commit_msg)

    def get_doc_filepath(self, doc_id):
        ga = self.create_git_action()
        return ga.path_for_doc(doc_id)


    def create_git_action_for_new_doc(self, new_doc_id=None):
        """Checks out master branch as a side effect"""
        ga = self.create_git_action()
        new_doc_id = self.check_new_doc_id(new_doc_id)
        self.register_doc_id(ga, new_doc_id)
        return ga, new_doc_id

    def check_new_doc_id(self, new_doc_id):
        "Helper for create_git_action_for_new_doc base class version just verifies that id is not None"
        assert new_doc_id is not None

def _invert_dict_list_val(d):
    o = {}
    for k, v in d.items():
        o.setdefault(v, []).append(k)
    for v in o.values():
        v.sort()
    return o
