#!/usr/bin/env python
from peyotl.utility import get_logger
import re
import os
from peyotl.git_storage import GitActionBase

_LOG = get_logger(__name__)

class PhylesystemFilepathMapper(object):
    # Allow simple slug-ified string with '{known-prefix}-{7-or-8-digit-id}-{7-or-8-digit-id}'
    # (8-digit ottids are probably years away, but allow them to be safe.)
    # N.B. currently only the 'additions' prefix is supported!
    id_pattern = re.compile(r'[a-zA-Z][a-zA-Z]_[0-9]+')
    wip_id_pattern = '.*_study_{i}_[0-9]+'
    branch_name_template = "{ghu}_study_{rid}"
    path_to_user_splitter = '_study_'

    def filepath_for_id(self, repo_dir, study_id):
        assert len(study_id) >= 4
        assert study_id[2] == '_'
        assert bool(PhylesystemFilepathMapper.id_pattern.match(study_id))
        frag = study_id[-2:]
        dest_topdir = study_id[:3] + frag
        dest_subdir = study_id
        dest_file = dest_subdir + '.json'
        return os.path.join(repo_dir, 'study', dest_topdir, dest_subdir, dest_file)

    def id_from_path(self, path):
        if path.startswith('study/'):
            try:
                return path.split('/')[-2]
            except:
                return None

phylesystem_path_mapper = PhylesystemFilepathMapper()

class PhylesystemGitAction(GitActionBase):
    def __init__(self,
                 repo,
                 max_file_size=None):
        """Create a GitAction object to interact with a Git repository
        PhylesystemGitAction(repo="/home/user/git/foo")
        """
        GitActionBase.__init__(self,
                               'nexson',
                               repo,
                               max_file_size,
                               path_mapper=phylesystem_path_mapper)

