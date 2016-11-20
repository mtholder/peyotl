#!/usr/bin/env python
from peyotl.utility import get_logger
import re
import os
from peyotl.git_storage import GitActionBase

# extract a study id from a git repo path (as returned by git-tree)

_LOG = get_logger(__name__)


class MergeException(Exception):
    pass


def get_filepath_for_namespaced_id(repo_dir, study_id):
    if len(study_id) < 4:
        while len(study_id) < 2:
            study_id = '0' + study_id
        study_id = 'pg_' + study_id
    elif study_id[2] != '_':
        study_id = 'pg_' + study_id
    from peyotl.phylesystem import STUDY_ID_PATTERN
    assert bool(STUDY_ID_PATTERN.match(study_id))
    frag = study_id[-2:]
    while len(frag) < 2:
        frag = '0' + frag
    dest_topdir = study_id[:3] + frag
    dest_subdir = study_id
    dest_file = dest_subdir + '.json'
    return os.path.join(repo_dir, 'study', dest_topdir, dest_subdir, dest_file)


def get_filepath_for_simple_id(repo_dir, study_id):
    return '{r}/study/{s}/{s}.json'.format(r=repo_dir, s=study_id)


def study_id_from_repo_path(path):
    if path.startswith('study/'):
        try:
            study_id = path.split('/')[-2]
            return study_id
        except:
            return None


class PhylesystemGitAction(GitActionBase):
    def __init__(self,
                 repo,
                 remote=None,
                 cache=None,  # pylint: disable=W0613
                 path_for_doc_fn=None,
                 max_file_size=None):
        """Create a GitAction object to interact with a Git repository

        Example:
        gd   = PhylesystemGitAction(repo="/home/user/git/foo")

        Note that this requires write access to the
        git repository directory, so it can create a
        lockfile in the .git directory.

        """
        GitActionBase.__init__(self,
                               'nexson',
                               repo,
                               remote,
                               cache,
                               path_for_doc_fn,
                               max_file_size,
                               id_from_path_fn=study_id_from_repo_path,
                               path_for_doc_id_fn=get_filepath_for_namespaced_id,
                               wip_id_pattern='.*_study_{i}_[0-9]+',
                               branch_name_template="{ghu}_study_{rid}",
                               path_to_user_splitter='_study_')
