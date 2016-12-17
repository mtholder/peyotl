#!/usr/bin/env python
# -*- coding: utf-8 -*-
import sys
from peyotl.git_versioned_doc_store_collection import clone_mirrors
try:
    repo_parent, remote_url_prefix = sys.argv[1:]
except:
    sys.exit('Expecting 2 arguments: a path to the repo parent and the prefix of the remote repo URL')

while remote_url_prefix.endswith('/'):
    remote_url_prefix = remote_url_prefix[:-1]

clone_mirrors(repo_parent, remote_url_prefix)
