#! /usr/bin/env python
# coding=utf-8
from peyotl.collections_store import _TreeCollectionStore
import unittest
from peyotl.test.support import pathmap
from peyotl.test.support import test_collection_indexing, test_changed_collections
import os

from peyotl.utility import get_logger

_LOG = get_logger(__name__)

_repos = pathmap.get_test_repos()
mc = _repos['mini_collections']


# TODO: filter repo list to just tree-collection shards? or rely on smart (failed) shard creation?
# _repos = {'mini_collections': mc}



@unittest.skipIf(not os.path.isdir(mc),
                 'Peyotl not configured for maintainer test of mini_collections.'
                 'Skipping this test is normal (for everyone other than maintainers).\n'
                 'See http://opentreeoflife.github.io/peyotl/maintainer/')
class TestTreeCollections(unittest.TestCase):
    def setUp(self):
        self.r = dict(_repos)

    def testSlugify(self):
        from peyotl.utility.str_util import slugify
        self.assertEqual('simple-test', slugify('Simple Test'))
        self.assertEqual('no-punctuation-allowed', slugify('No punctuation allowed!?'))
        self.assertEqual('no-extra-spaces-', slugify('No \t extra   spaces   '))
        self.assertEqual('untitled', slugify(''))
        self.assertEqual('untitled', slugify('!?'))
        # TODO: allow broader Unicode strings and their capitalization rules?
        # self.assertEqual(u'километр', slugify(u'Километр'))
        self.assertEqual(u'untitled', slugify(u'Километр'))  # no support for now

    def testInit(self):
        c = _TreeCollectionStore(repos_dict=self.r)
        self.assertEqual(1, len(c._shards))

    def testURL(self):
        c = _TreeCollectionStore(repos_dict=self.r)
        self.assertTrue(c.get_public_url('TestUserB/fungal-trees').endswith('ngal-trees.json'))

    def testCollectionCreation(self):
        c = _TreeCollectionStore(repos_dict=self.r)
        # TODO: create a new collection with a unique name, confirm it exists

    def testCollectionCopying(self):
        c = _TreeCollectionStore(repos_dict=self.r)
        # TODO: copy an existing study under the same user, confirm it's in the right place
        # TODO: copy an existing study under a new user, confirm it's in the right place

    def testNewCollectionIds(self):
        # We assign each new collection a unique id based on the owner's userid +
        # the slugified name, serializing with $NAME-2, etc if this id already exists.
        c = _TreeCollectionStore(repos_dict=self.r)
        # TODO: fetch an existing study, copy to the other user (id should reflect new username)
        # TODO: fetch an existing study, save a copy alongside it (should nudge id via serialization)
        # TODO: create a new study (with the same name) alongside thes (should nudge id via serialization)

    def testCollectionDeletion(self):
        c = _TreeCollectionStore(repos_dict=self.r)
        # TODO: create a new collection with a unique name, confirm it exists
        # TODO: delete the collection, make sure it's gone

    def testCollectionIndexing(self):
        test_collection_indexing(self, _TreeCollectionStore(repos_dict=self.r))

    def testChangedCollectionsShell(self):
        test_changed_collections(self, _TreeCollectionStore(repos_dict=self.r))

if __name__ == "__main__":
    unittest.main(verbosity=5)
