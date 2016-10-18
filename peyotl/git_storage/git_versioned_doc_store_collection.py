#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""A class that can detect the git-managed doc stores in directory
and create the Python objects that provide an interface for them.
"""
from __future__ import print_function
from peyotl.utility import get_logger, expand_abspath
from peyotl.git_storage.helper import dir_to_repos_dict
from peyotl.collections_store.collections_umbrella import create_tree_collection_umbrella
from peyotl.amendments.amendments_umbrella import create_taxonomic_amendments_umbrella
from peyotl.phylesystem.phylesystem_umbrella import create_phylesystem_umbrella
from peyotl.git_storage.git_action import read_remotes_config
import os
import threading

_DOCSTORE_SUBDIR_NAME_TO_TYPE = {'amendments': ('taxonomic amendments', create_taxonomic_amendments_umbrella),
                                 'collections-by-owner': ('tree collections', create_tree_collection_umbrella),
                                 'study': ('phylogenetic studies', create_phylesystem_umbrella)
                                 }
_DOCSTORE_TYPE_NAME = [_i[0] for _i in _DOCSTORE_SUBDIR_NAME_TO_TYPE.values()]

_LOG = get_logger(__name__)
_UMBRELLA_SINGLETON_MAP = {}
_UMBRELLA_SINGLETON_MAP_LOCK = threading.Lock()


def group_subdirs_by_docstore_type(directory):
    """When `directory` is a parent of git repos, this function will
    return a dict mapping a string from ['taxonomic amendments',
    'tree collections', 'phylogenetic studies'] to a tuple that holds
    the factory function for an umbrella type at element 0 and
    a list of paths to the repositories that are
    diagnosed to be of that type.

    This function uses the presences of an "amendments", "collections-by-owner", or "study"
    subdirectory to diagnose a git repository's type.

    Warnings are emitted for any git repo found that cannot be mapped to one type, and these
        directories are not returned.
    """
    by_type = {}
    potential_shards = dir_to_repos_dict(directory)
    for d in potential_shards.values():
        diagnosed_type = []
        for subdir_name, type_diag in _DOCSTORE_SUBDIR_NAME_TO_TYPE.items():
            possible_doc_dir = os.path.join(d, subdir_name)
            if os.path.isdir(possible_doc_dir):
                diagnosed_type.append(type_diag)
        if len(diagnosed_type) == 0:
            _LOG.warn('git repository at "{}" could not be assigned to any docstore type'.format(d))
        elif len(diagnosed_type) > 1:
            _LOG.warn('git repository at "{}" could not matches multiple docstore type'.format(d))
        else:
            type_name, umbrella_type = diagnosed_type[0]
            dir_list = by_type.setdefault(type_name, (umbrella_type, []))[1]
            dir_list.append(d)
    return by_type


def group_subdirs_and_mirrors_by_docstore_type(parent, mirror_dir=None):
    """Takes a parent "shards" parent and returns a dict with keys that
    are strings from the list ['taxonomic amendments', 'tree collections', 'phylogenetic studies']
    and values which are a list of:
        The "umbrella" type for the git-based doc store, and
        a list of pairs of directories. One element for each shard of that type. The first
            element in each list is subdir of `parent` that is a git repo of this type. The
            second element is None (if there is no push mirror for that shard) and the
            path to the mirror repo (if there is a push mirror for the shard)

    The `mirror_dir` argument is the parent of the mirror repos. If it is None, then `parent`/mirror
    is used.

    If an appropriate git repo is found in the mirror parent for a doctore type, then a mirror must be
        set up to pull from the non-mirror and to push to a remote called GitHubRemote. Improper
        mirroring set up will result in a RuntimeError being raised.
    """
    parent = expand_abspath(parent)
    sub_by_type = group_subdirs_by_docstore_type(parent)
    if mirror_dir is None:
        mirror_dir = os.path.join(parent, 'mirror')
    mirror_dir = expand_abspath(mirror_dir)
    if os.path.isdir(mirror_dir):
        mirror_sub_by_type = group_subdirs_by_docstore_type(mirror_dir)
    else:
        mirror_sub_by_type = {}
    grouped = {}
    for t_name, type_path_list in sub_by_type.items():
        umbrella_type = type_path_list[0]
        m_tpl = mirror_sub_by_type.get(t_name)
        gv = [umbrella_type, []]
        list_of_working_mirror_paths = gv[-1]
        rpl = type_path_list[1]
        rpbs = {os.path.split(i)[-1]: i for i in rpl}
        shard_names = rpbs.keys()
        shard_names.sort()
        if m_tpl:
            assert m_tpl[0] == umbrella_type
            mpl = m_tpl[1]
            mpbs = {os.path.split(i)[-1]: i for i in mpl}
            for sn in shard_names:
                p = rpbs[sn]
                mfp = mpbs.get(sn)
                if mfp is None:
                    raise RuntimeError('Did not find a mirror for {} in "{}"'.format(sn, mirror_dir))
                mirror_remotes_dict = read_remotes_config(mfp)
                ori_path = mirror_remotes_dict.get('origin', {}).get('fetch')
                gh_path = mirror_remotes_dict.get('GitHubRemote', {}).get('push')
                if p != ori_path:
                    raise RuntimeError('The mirror at "{}" is not setup to fetch from "{}"'.format(mfp, p))
                if gh_path is None:
                    raise RuntimeError('The mirror at "{}" is not setup to push to a "GitHubRemote" remote'.format(mfp))
                list_of_working_mirror_paths.append([p, mfp])
        else:
            for sn in shard_names:
                p = rpbs[sn]
                list_of_working_mirror_paths.append([p, None])
        grouped[t_name] = gv
    return grouped


class GitVersionedDocStoreCollection(object):
    def __init__(self, repo_parent, phylesystem_study_id_prefix='ot_'):
        self.repo_parent = expand_abspath(repo_parent)
        self.taxon_amendments = None
        self.tree_collections = None
        self.phylesystem = None
        by_type = group_subdirs_and_mirrors_by_docstore_type(self.repo_parent)
        if not by_type:
            raise ValueError('repo_parent "{}" does not contain any git shards'.format(repo_parent))
        for type_name, blob in by_type.items():
            factory, shard_mirror_pair_list = blob
            if type_name == 'phylogenetic studies':
                self.phylesystem = factory(shard_mirror_pair_list=shard_mirror_pair_list,
                                           new_study_prefix=phylesystem_study_id_prefix)
            elif type_name == 'tree collections':
                self.tree_collections = factory(shard_mirror_pair_list)
            elif type_name == 'taxonomic amendments':
                self.taxon_amendments = factory(shard_mirror_pair_list)
            else:
                assert False, 'Unrecognized doc type: {}'.format(type_name)


def create_doc_store_wrapper(shards_dir, phylesystem_study_id_prefix='ot_'):
    """Factory function for a GitVersionedDocStoreCollection for a shards_dir.
    Each shards_dir maps to a singleton instance of GitVersionedDocStoreCollection.
    The `phylesystem_study_id_prefix` becomes the Phylesystem's new_study_prefix.
    """
    ap = expand_abspath(shards_dir)
    with _UMBRELLA_SINGLETON_MAP_LOCK:
        umb = _UMBRELLA_SINGLETON_MAP.get(ap)
        if umb is None:
            umb = GitVersionedDocStoreCollection(repo_parent=ap,
                                                 phylesystem_study_id_prefix=phylesystem_study_id_prefix)
            _UMBRELLA_SINGLETON_MAP[ap] = umb
    return umb


def clone_mirrors(repo_parent, remote_url_prefix, name_set=None):
    """Looks for all of the git-versioned doc stores in `repo_parent` that
        do not have push mirrors.
    Clones each to push to `remote_url_prefix`/repo_name.git
    If `name_set` is not None, then only repo_names in name_set will be cloned.
    """
    from peyotl.git_storage import GitActionBase
    repo_parent = expand_abspath(repo_parent)
    x = group_subdirs_and_mirrors_by_docstore_type(repo_parent)
    mirror_dir = os.path.join(repo_parent, 'mirror')
    for fact_shard_mirror_pairs in x.values():
        shard_mirror_pairs_list = fact_shard_mirror_pairs[1]
        for repo_filepath, mirror_path in shard_mirror_pairs_list:
            if mirror_path is None:
                if not os.path.isdir(mirror_dir):
                    os.makedirs(mirror_dir)
                repo_name = os.path.split(repo_filepath)[-1]
                if (name_set is not None) and (repo_name not in name_set):
                    continue
                expected_push_mirror_repo_path = os.path.join(mirror_dir, repo_name)
                GitActionBase.clone_repo(mirror_dir,
                                         repo_name,
                                         repo_filepath)
                if not os.path.isdir(expected_push_mirror_repo_path):
                    e_msg = 'git clone in mirror bootstrapping did not produce a directory at {}'
                    e = e_msg.format(expected_push_mirror_repo_path)
                    raise ValueError(e)
                remote_url = remote_url_prefix + '/' + repo_name + '.git'
                GitActionBase.add_remote(expected_push_mirror_repo_path, 'GitHubRemote', remote_url)
