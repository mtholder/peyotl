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
        if len(study_id) < 4:
            while len(study_id) < 2:
                study_id = '0' + study_id
            study_id = 'pg_' + study_id
        elif study_id[2] != '_':
            study_id = 'pg_' + study_id
        from peyotl.phylesystem import PhylesystemFilepathMapper
        assert bool(PhylesystemFilepathMapper.id_pattern.match(study_id))
        frag = study_id[-2:]
        while len(frag) < 2:
            frag = '0' + frag
        dest_topdir = study_id[:3] + frag
        dest_subdir = study_id
        dest_file = dest_subdir + '.json'
        return os.path.join(repo_dir, 'study', dest_topdir, dest_subdir, dest_file)

    def id_from_path(self, path):
        if path.startswith('study/'):
            try:
                study_id = path.split('/')[-2]
                return study_id
            except:
                return None


phylesystem_path_mapper = PhylesystemFilepathMapper()

class PhylesystemGitAction(GitActionBase):
    def __init__(self,
                 repo,
                 remote=None,
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
                               max_file_size,
                               path_mapper=phylesystem_path_mapper)

