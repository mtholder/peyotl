#!/usr/bin/env python
# -*- coding: utf-8 -*-
from peyotl.utility.imports import SafeConfigParser
from peyotl.test.support.pathmap import get_test_repo_parent
from peyotl.git_storage.git_versioned_doc_store_collection import clone_mirrors
import sys
import os

repo_parent = get_test_repo_parent()
if not os.path.exists(repo_parent):
    sys.exit('test repos not found!\n')
config_path = os.path.join('tests', 'local_repos_tests', 'test_mirror.conf')
if not os.path.isfile(config_path):
    sys.exit('Mirrors not set up because "{}" was not found. See "{}.example"\n'.format(config_path, config_path))
cfg = SafeConfigParser()
cfg.read(config_path)
try:
    rem_pref = cfg.get('git_stores', 'remote_prefix')
except:
    sys.exit(
        'Mirrors not set up because no git_stores section with a remote_prefix was found in "{}"'.format(config_path))

try:
    clone_mirrors(repo_parent, rem_pref)
except:
    sys.stderr.write('Error in setting up test clone mirrors')
    raise
