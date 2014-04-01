#! /usr/bin/env python
import unittest
import codecs
import json
from peyotl.test.support import pathmap
from peyotl.utility import get_logger
_LOG = get_logger(__name__)

_MINI_PHYL_SHA1 = 'aa8964b55bfa930a91af7a436f55f0acdc94b918'
_SID = '9'
_AUTH = {
    'name': 'test_name',
    'email': 'test_email@example.org',
    'login': 'test_gh_login',
}
class TestPhylesystemMirror(unittest.TestCase):
    def testMirrorConfig(self):
        p = pathmap.get_test_phylesystem()
        if p is None:
            _LOG.debug('Test skipped for lack of push-enabled test phyleystem')
        acurr_obj, sha, wip_map = p.return_study(_SID, return_WIP_map=True)
        _LOG.debug('test sha = "{}"'.format(sha))
        self.assertEquals(wip_map.keys(), ['master'])
        ac = acurr_obj['nexml'].get("^acount", 0)
        # add a second commit that should merge to master
        ac += 1
        acurr_obj['nexml']["^acount"] = ac
        v1b = p.commit_and_try_merge2master(acurr_obj,
                                            _SID,
                                            _AUTH,
                                            sha)
        self.assertFalse(v1b['merge_needed'])
        p.push_study_to_remote('GitHubRemote', _SID)
if __name__ == "__main__":
    unittest.main()