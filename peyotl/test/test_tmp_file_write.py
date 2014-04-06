#! /usr/bin/env python
from peyotl.phylesystem.git_actions import GitAction
from peyotl import phylesystem
import unittest
from peyotl.nexson_syntax import read_as_json
from peyotl.test.support import pathmap

n = read_as_json(pathmap.json_source_path('1003.json'))

reponame = phylesystem.get_repos().keys()[0]
repodir = phylesystem.get_repos()[reponame]

class TestCreate(unittest.TestCase):
    def testWriteStudy(self):
        GitAction(repodir)
        
if __name__ == "__main__":
    unittest.main()