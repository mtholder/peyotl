#! /usr/bin/env python
from peyotl.phylesystem.git_workflows import acquire_lock_raise, \
                                             commit_and_try_merge2master, \
                                             delete_study, \
                                             GitWorkflowError, \
                                             merge_from_master
from peyotl.phylesystem import Phylesystem
import unittest
import codecs
import json
import copy
from peyotl.nexson_syntax import read_as_json
from peyotl.test.support import pathmap
from peyotl.utility import get_logger
_LOG = get_logger(__name__)

phylesystem = Phylesystem(pathmap.get_test_repos())

_MINI_PHYL_SHA1 = 'aa8964b55bfa930a91af7a436f55f0acdc94b918'
_SID = '9'
_AUTH = {
    'name': 'test_name',
    'email': 'test_email@example.org',
    'login': 'test_gh_login',
}

class TestPhylesystemC(unittest.TestCase):

    def testConflicting(self):
        ga = phylesystem.create_git_action(_SID)
        ga.acquire_lock()
        try:
            curr, sha, wip_map = ga.return_study(_SID, return_WIP_map=True)
        finally:
            ga.release_lock()
        _LOG.debug('test sha = "{}"'.format(sha))
        conflicted_branch_name = 'test_gh_login_study_9_0'
        self.assertTrue(conflicted_branch_name in wip_map.keys())
        commit_sha = wip_map[conflicted_branch_name]
        mblob = merge_from_master(ga, _SID, _AUTH, commit_sha)
        self.assertEqual(mblob["error"], 0)
        self.assertEqual(mblob["resource_id"], _SID)

if __name__ == "__main__":
    unittest.main()
