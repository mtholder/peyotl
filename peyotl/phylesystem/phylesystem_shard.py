import os
import re
import json
import codecs
from threading import Lock
from peyotl.utility import get_config_setting, get_logger
from peyotl.git_storage.git_shard import (GitShard,
                                          TypeAwareGitShard,
                                          FailedShardCreationError,
                                          _invert_dict_list_val)

_LOG = get_logger(__name__)
# class PhylesystemShardBase(object):

doc_holder_subpath = 'study'



class PhylesystemShardProxy(GitShard):
    """Proxy for shard when interacting with external resources if given the configuration of a remote Phylesystem
    """

    def __init__(self, config):
        GitShard.__init__(self, config['name'])
        self.repo_nexml2json = config['repo_nexml2json']
        d = {}
        for study in config['studies']:
            kl = study['keys']
            if len(kl) > 1:
                _LOG.warn("ID aliases are no longer supported withing the Shards")
            for k in study['keys']:
                d[k] = (self.name, self.path, self.path + '/study/' + study['relpath'])
        self.study_index = d

    # rename some generic members in the base class, for clarity and backward compatibility
    @property
    def repo_nexml2json(self):
        return self.assumed_doc_version

    @repo_nexml2json.setter
    def repo_nexml2json(self, val):
        self.assumed_doc_version = val

    @property
    def study_index(self):
        return self.doc_index

    @study_index.setter
    def study_index(self, val):
        self._doc_index = val

    @property
    def new_study_prefix(self):
        return self.new_doc_prefix

    def get_study_ids(self):
        return self.get_doc_ids()


def diagnose_repo_nexml2json(shard):
    """Optimistic test for Nexson version in a shard (tests first study found)"""
    with shard._index_lock:
        fp = next(iter(shard.study_index.values()))[2]
    with codecs.open(fp, mode='r', encoding='utf-8') as fo:
        fj = json.load(fo)
        from peyotl.nexson_syntax import detect_nexson_version
        return detect_nexson_version(fj)


def refresh_study_index(shard, initializing=False):
    from peyotl.phylesystem.helper import create_id2study_info, \
        get_filepath_for_namespaced_id
    d = create_id2study_info(shard.doc_dir, shard.name)
    shard.filepath_for_doc_id_fn = get_filepath_for_namespaced_id
    shard.has_aliases = False
    shard.study_index = d


class PhylesystemShard(TypeAwareGitShard):
    """Wrapper around a git repo holding nexson studies.
    Raises a ValueError if the directory does not appear to be a PhylesystemShard.
    Raises a RuntimeError for errors associated with misconfiguration."""
    from peyotl.phylesystem.git_actions import PhylesystemGitAction
    def __init__(self,
                 name,
                 path,
                 assumed_doc_version=None,
                 git_ssh=None,
                 pkey=None,
                 git_action_class=PhylesystemGitAction,
                 push_mirror_repo_path=None,
                 infrastructure_commit_author='OpenTree API <api@opentreeoflife.org>',
                 max_file_size=None):
        if max_file_size is None:
            max_file_size = get_config_setting('phylesystem', 'max_file_size')
        TypeAwareGitShard.__init__(self,
                                   name=name,
                                   path=path,
                                   doc_holder_subpath=doc_holder_subpath,
                                   assumed_doc_version=assumed_doc_version,
                                   detect_doc_version_fn=diagnose_repo_nexml2json,  # version detection
                                   refresh_doc_index_fn=refresh_study_index,  # populates 'study_index'
                                   git_ssh=git_ssh,
                                   pkey=pkey,
                                   git_action_class=git_action_class,
                                   push_mirror_repo_path=push_mirror_repo_path,
                                   infrastructure_commit_author=infrastructure_commit_author,
                                   max_file_size=max_file_size)
        self._doc_counter_lock = Lock()
        self._id_minting_file = os.path.join(path, 'next_study_id.json')
        self.filepath_for_global_resource_fn = lambda frag: os.path.join(path, frag)
        self._next_study_id = None
    def can_mint_new_docs(self):
        return self._new_doc_prefix is not None

    # rename some generic members in the base class, for clarity and backward compatibility
    @property
    def next_study_id(self):
        return self._next_study_id

    @property
    def iter_study_objs(self):
        return self.iter_doc_objs

    @property
    def iter_study_filepaths(self):
        return self.iter_doc_filepaths

    @property
    def get_changed_studies(self):
        return self.get_changed_docs

    @property
    def known_prefixes(self):
        if self._known_prefixes is None:
            self._known_prefixes = self._diagnose_prefixes()
        return self._known_prefixes

    @property
    def new_study_prefix(self):
        return self._new_doc_prefix

    @property
    def study_index(self):
        return self.doc_index

    @study_index.setter
    def study_index(self, val):
        self._doc_index = val

    @property
    def repo_nexml2json(self):
        return self.assumed_doc_version

    @repo_nexml2json.setter
    def repo_nexml2json(self, val):
        self.assumed_doc_version = val

    def get_study_ids(self):
        return self.get_doc_ids()

    # Type-specific configuration for backward compatibility
    # (config is visible to API consumers via /phylesystem_config)
    def write_configuration(self, out, secret_attrs=False):
        """Type-specific configuration for backward compatibility"""
        key_order = ['name', 'path', 'git_dir', 'study_dir', 'repo_nexml2json',
                     'git_ssh', 'pkey', 'has_aliases', '_next_study_id',
                     'number of studies']
        cd = self.get_configuration_dict(secret_attrs=secret_attrs)
        for k in key_order:
            if k in cd:
                out.write('  {} = {}'.format(k, cd[k]))
        out.write('  studies in alias groups:\n')
        for o in cd['studies']:
            out.write('    {} ==> {}\n'.format(o['keys'], o['relpath']))

    def get_configuration_dict(self, secret_attrs=False):
        """Type-specific configuration for backward compatibility"""
        rd = {'name': self.name,
              'path': self.path,
              'git_dir': self.git_dir,
              'repo_nexml2json': self.repo_nexml2json,  # assumed_doc_version
              'study_dir': self.doc_dir,
              'git_ssh': self.git_ssh, }
        if self._next_study_id is not None:
            rd['_next_study_id'] = self._next_study_id,
        if secret_attrs:
            rd['pkey'] = self.pkey
        with self._index_lock:
            si = self.study_index
        r = _invert_dict_list_val(si)
        key_list = list(r.keys())
        rd['number of studies'] = len(key_list)
        key_list.sort()
        m = []
        for k in key_list:
            v = r[k]
            fp = k[2]
            assert fp.startswith(self.doc_dir)
            rp = fp[len(self.doc_dir) + 1:]
            m.append({'keys': v, 'relpath': rp})
        rd['studies'] = m
        return rd

    def _determine_next_study_id(self):
        """Return the numeric part of the newest study_id

        Checks out master branch as a side effect!
        """
        if self._doc_counter_lock is None:
            self._doc_counter_lock = Lock()
        prefix = self._new_doc_prefix
        lp = len(prefix)
        n = 0
        # this function holds the lock for quite awhile,
        #   but it only called on the first instance of
        #   of creating a new study
        with self._doc_counter_lock:
            with self._index_lock:
                for k in self.study_index.keys():
                    if k.startswith(prefix):
                        try:
                            pn = int(k[lp:])
                            if pn > n:
                                n = pn
                        except:
                            pass
            nsi_contents = self._read_master_branch_resource(self._id_minting_file, is_json=True)
            try:
                self._next_study_id = nsi_contents['next_study_id']
            except:
                raise RuntimeError("Could not read 'next_study_id' from {}".format(self._id_minting_file))
            if self._next_study_id <= n:
                m = 'next_study_id in {} is set lower than the ID of an existing study!'
                m = m.format(self._id_minting_file)
                raise RuntimeError(m)

    def _advance_new_study_id(self):
        """ ASSUMES the caller holds the _doc_counter_lock !
        Returns the current numeric part of the next study ID, advances
        the counter to the next value, and stores that value in the
        file in case the server is restarted.
        """
        c = self._next_study_id
        self._next_study_id = 1 + c
        content = u'{"next_study_id": %d}\n' % self._next_study_id
        # The content is JSON, but we hand-rolled the string above
        #       so that we can use it as a commit_msg
        self._write_master_branch_resource(content,
                                           self._id_minting_file,
                                           commit_msg=content,
                                           is_json=False)
        return c

    def _diagnose_prefixes(self):
        """Returns a set of all of the prefixes seen in the main document dir
        """
        from peyotl.phylesystem import STUDY_ID_PATTERN
        p = set()
        for name in os.listdir(self.doc_dir):
            if STUDY_ID_PATTERN.match(name):
                p.add(name[:3])
        return p

    def _mint_new_study_id(self):
        """Checks out master branch as a side effect"""
        # studies created by the OpenTree API start with ot_,
        # so they don't conflict with new study id's from other sources
        with self._doc_counter_lock:
            c = self._advance_new_study_id()
        # @TODO. This form of incrementing assumes that
        #   this codebase is the only service minting
        #   new study IDs!
        return "{p}{c:d}".format(p=self._new_doc_prefix, c=c)

    def create_git_action_for_new_study(self, new_study_id=None):
        """Checks out master branch as a side effect"""
        ga = self.create_git_action()
        if new_study_id is None:
            new_study_id = self._mint_new_study_id()
        self.register_doc_id(ga, new_study_id)
        return ga, new_study_id

    def _create_git_action_for_global_resource(self):
        return self._ga_class(repo=self.path,
                              git_ssh=self.git_ssh,
                              pkey=self.pkey,
                              path_for_doc_fn=self.filepath_for_global_resource_fn,
                              max_file_size=self.max_file_size)
