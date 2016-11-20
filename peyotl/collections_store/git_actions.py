#!/usr/bin/env python
from peyotl.utility import get_logger
from peyotl.git_storage import GitActionBase
import re
_LOG = get_logger(__name__)

class CollectionsFilepathMapper(object):
    id_pattern =  re.compile(r'^[a-zA-Z0-9-]+/[a-z0-9-]+$')
    wip_id_template = r'.*_collection_{i}_[0-9]+',
    branch_name_template = "{ghu}_collection_{rid}",
    path_to_user_splitter = '_collection_'

    def filepath_for_id(self, repo_dir, collection_id):
        assert bool(CollectionsFilepathMapper.id_pattern.match(collection_id))
        return '{r}/collections-by-owner/{s}.json'.format(r=repo_dir, s=collection_id)

    def id_from_path(self, path):
        doc_parent_dir = 'collections-by-owner/'
        if path.startswith(doc_parent_dir):
            return path.split(doc_parent_dir)[1]

collections_path_mapper = CollectionsFilepathMapper()

class TreeCollectionsGitAction(GitActionBase):
    def __init__(self,
                 repo,
                 max_file_size=None):
        """GitActionBase subclass to interact with a Git repository
        TreeCollectionsGitAction(repo="/home/user/git/foo")
        """
        GitActionBase.__init__(self,
                               'collection',
                               repo,
                               max_file_size,
                               path_mapper=collections_path_mapper)
