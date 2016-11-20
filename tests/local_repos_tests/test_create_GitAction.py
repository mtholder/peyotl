#! /usr/bin/env python
from peyotl.phylesystem.phylesystem_shard import create_phylesystem_git_action
import unittest
from peyotl.git_storage.helper import get_repos
try:
    r = get_repos()
    HAS_LOCAL_PHYLESYSTEM_REPOS = True
except:
    HAS_LOCAL_PHYLESYSTEM_REPOS = False


class TestCreate(unittest.TestCase):
    @unittest.skipIf(not HAS_LOCAL_PHYLESYSTEM_REPOS,
                     'only available if you are have a [phylesystem] section'
                     ' with "parent" variable in your peyotl config')
    def testConstructor(self):
        self.reponame = list(get_repos().keys())[0]
        self.repodir = get_repos()[self.reponame]
        gd = create_phylesystem_git_action(self.repodir)
        gd.acquire_lock()
        gd.release_lock()
        gd.checkout_master()
        self.assertEqual(gd.current_branch(), "master")

if __name__ == "__main__":
    unittest.main()
