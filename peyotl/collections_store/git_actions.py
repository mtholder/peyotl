#!/usr/bin/env python
from peyotl.utility import get_logger
import re
from peyotl.git_storage import GitActionBase

# extract a collection id from a git repo path (as returned by git-tree)

_LOG = get_logger(__name__)


class MergeException(Exception):
    pass


def get_filepath_for_id(repo_dir, collection_id):
    from peyotl.collections_store import COLLECTION_ID_PATTERN
    assert bool(COLLECTION_ID_PATTERN.match(collection_id))
    return '{r}/collections-by-owner/{s}.json'.format(r=repo_dir, s=collection_id)


def collection_id_from_repo_path(path):
    doc_parent_dir = 'collections-by-owner/'
    if path.startswith(doc_parent_dir):
        try:
            collection_id = path.split(doc_parent_dir)[1]
            return collection_id
        except:
            return None


class TreeCollectionsGitAction(GitActionBase):
    def __init__(self,
                 repo,
                 remote=None,
                 cache=None,  # pylint: disable=W0613
                 path_for_doc_fn=None,
                 max_file_size=None):
        """GitActionBase subclass to interact with a Git repository

        Example:
        gd   = TreeCollectionsGitAction(repo="/home/user/git/foo")

        Note that this requires write access to the
        git repository directory, so it can create a
        lockfile in the .git directory.

        """
        GitActionBase.__init__(self,
                               'collection',
                               repo,
                               remote,
                               cache,
                               path_for_doc_fn,
                               max_file_size,
                               id_from_path_fn=collection_id_from_repo_path,
                               path_for_doc_id_fn=get_filepath_for_id,
                               wip_id_pattern=r'.*_collection_{i}_[0-9]+',
                               branch_name_template="{ghu}_collection_{rid}",
                               path_to_user_splitter='_collection_')
