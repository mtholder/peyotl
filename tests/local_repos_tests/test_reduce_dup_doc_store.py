#! /usr/bin/env python
# -*- coding: utf-8 -*-
# put tests here that use local phylesystem
# see http://opentreeoflife.github.io/peyotl/maintainer/ for setup

from peyotl.utility.input_output import read_as_json
from peyotl.git_storage.git_versioned_doc_store_collection import create_doc_store_wrapper
import unittest
from peyotl.test.support.pathmap import get_test_repo_parent
import os

from peyotl.utility import get_logger
from peyotl.utility.str_util import slugify

_LOG = get_logger(__name__)

repo_par = get_test_repo_parent()


# TODO: filter repo list to just phylesystem shards? or rely on smart (failed) shard creation?
# _repos = {s: _repos[s] for s in _repos if s in ('mini_system', 'mini_phyl',)}

# pylint: disable=W0212
@unittest.skipIf((not os.path.isdir(repo_par)),
                 'Peyotl not configured for maintainer test of mini_* repos.'
                 'Skipping this test is normal (for everyone other than MTH and EJBM).\n'
                 'See http://opentreeoflife.github.io/peyotl/maintainer/ ')
class TestPhylesystem(unittest.TestCase):
    def setUp(self):
        self.wrapper = create_doc_store_wrapper(repo_par)

    def testInit(self):
        self.assertEqual(2, len(self.wrapper.phylesystem._shards))
        self.assertEqual(1, len(self.wrapper.taxon_amendments._shards))
        self.assertEqual(1, len(self.wrapper.tree_collections._shards))

    def testStudyIndexing(self):
        p = self.wrapper.phylesystem
        k = list(p._doc2shard_map.keys())
        k.sort()
        self.assertEqual(k, ['xy_10', 'xy_13', 'zz_11', 'zz_112'])

    def testURL(self):
        p =self.wrapper.phylesystem
        self.assertTrue(p.get_public_url('xy_10').endswith('xy_10.json'))
        self.assertTrue(p.get_public_url('zz_112').endswith('zz_112.json'))
        a = self.wrapper.taxon_amendments
        self.assertTrue(a.get_public_url('additions-5000000-5000003').endswith('-5000003.json'))
        c = self.wrapper.tree_collections
        self.assertTrue(c.get_public_url('TestUserB/fungal-trees').endswith('ngal-trees.json'))

    def testStudyIds(self):
        p = self.wrapper.phylesystem
        k = list(p.get_study_ids())
        k.sort()
        self.assertEqual(k, ['xy_10', 'xy_13', 'zz_11', 'zz_112'])

    def testNextStudyIds(self):
        p = self.wrapper.phylesystem
        mf = p._growing_shard._id_minting_file
        nsi = p._mint_new_study_id()
        self.assertEqual(int(nsi.split('_')[-1]) + 1, read_as_json(mf)['next_study_id'])
        self.assertTrue(nsi.startswith('zz_'))

    def testChangedStudies(self):
        p = self.wrapper.phylesystem
        p.pull()  # get the full git history
        changed = p.get_changed_studies('5f50b669cb4867d39e9a85e7fd1e2aa8e9a3242b')
        self.assertEqual({'xy_13', 'xy_10'}, changed)
        changed = p.get_changed_studies('5f50b669cb4867d39e9a85e7fd1e2aa8e9a3242b', ['zz_11'])
        self.assertEqual(set(), changed)
        changed = p.get_changed_studies('5f50b669cb4867d39e9a85e7fd1e2aa8e9a3242b', ['zz_112'])
        self.assertEqual(set(), changed)
        self.assertRaises(ValueError, p.get_changed_studies, 'bogus')

    def testIterateStudies(self):
        p = self.wrapper.phylesystem
        k = list(p.get_study_ids())
        count = 0
        for study_id, file_path in p.iter_study_filepaths():
            count += 1
        self.assertEqual(count, len(k))
        count = 0
        for study_id, n in p.iter_study_objs():
            count += 1
        self.assertEqual(count, len(k))

    def testAmendmentIndexing(self):
        a = self.wrapper.taxon_amendments
        k = list(a._doc2shard_map.keys())
        k.sort()
        expected = ['additions-5000000-5000003']
        # TODO: populate with more test data?
        self.assertEqual(k, expected)

    def testAmendmentIds(self):
        a = self.wrapper.taxon_amendments
        k = list(a.get_doc_ids())
        k.sort()
        expected = ['additions-5000000-5000003']  # TODO: add more docs, to test sorting?
        self.assertEqual(k, expected)


    def testChangedAmendments(self):
        a = self.wrapper.taxon_amendments
        a.pull()  # get the full git history
        # this SHA only affected other files (not docs)
        # REMINDER: This will list all changed files *since* the stated SHA; results
        # will probably change if more work is done in the mini_amendments repo!
        # TODO: add a test with the HEAD commit SHA that should get no changes
        # check for known changed amendments in this repo (ignoring other changed files)
        changed = a.get_changed_docs('59e6d2d2ea62aa1ce784d29bdd43e74aa80d07d4')
        _LOG.debug('changed = {}'.format(changed))
        self.assertEqual({u'additions-5000000-5000003.json'}, changed)
        # check a doc that changed (against whitelist)
        changed = a.get_changed_docs('59e6d2d2ea62aa1ce784d29bdd43e74aa80d07d4',
                                          [u'additions-5000000-5000003.json'])
        self.assertEqual({u'additions-5000000-5000003.json'}, changed)
        # checking a bogus doc id should work, but find nothing
        changed = a.get_changed_docs('59e6d2d2ea62aa1ce784d29bdd43e74aa80d07d4',
                                          [u'non-existing-amendment.json'])
        self.assertEqual(set(), changed)
        # passing a foreign (or nonsense) SHA should raise a ValueError
        self.assertRaises(ValueError, a.get_changed_docs, 'bogus-SHA')


    def testSlugify(self):
        self.assertEqual('simple-test', slugify('Simple Test'))
        self.assertEqual('no-punctuation-allowed', slugify('No punctuation allowed!?'))
        self.assertEqual('no-extra-spaces-', slugify('No \t extra   spaces   '))
        self.assertEqual('untitled', slugify(''))
        self.assertEqual('untitled', slugify('!?'))
        # TODO: allow broader Unicode strings and their capitalization rules?
        # self.assertEqual(u'километр', slugify(u'Километр'))
        self.assertEqual(u'untitled', slugify(u'Километр'))  # no support for now



    def testCollectionIndexing(self):
        c = self.wrapper.tree_collections
        k = list(c._doc2shard_map.keys())
        k.sort()
        expected = ['TestUserB/fungal-trees', 'TestUserB/my-favorite-trees',
                    'test-user-a/my-favorite-trees', 'test-user-a/trees-about-bees']
        self.assertEqual(k, expected)


    def testCollectionIds(self):
        c = self.wrapper.tree_collections
        k = list(c.get_doc_ids())
        k.sort()
        expected = ['TestUserB/fungal-trees', 'TestUserB/my-favorite-trees',
                    'test-user-a/my-favorite-trees', 'test-user-a/trees-about-bees']
        self.assertEqual(k, expected)


    def testCollectionCreation(self):
        c = self.wrapper.tree_collections
        # TODO: create a new collection with a unique name, confirm it exists


    def testCollectionCopying(self):
        c = self.wrapper.tree_collections
        # TODO: copy an existing study under the same user, confirm it's in the right place
        # TODO: copy an existing study under a new user, confirm it's in the right place


    def testNewCollectionIds(self):
        # We assign each new collection a unique id based on the owner's userid +
        # the slugified name, serializing with $NAME-2, etc if this id already exists.
        c = self.wrapper.tree_collections
        # TODO: fetch an existing study, copy to the other user (id should reflect new username)
        # TODO: fetch an existing study, save a copy alongside it (should nudge id via serialization)
        # TODO: create a new study (with the same name) alongside thes (should nudge id via serialization)


    def testCollectionDeletion(self):
        c = self.wrapper.tree_collections
        # TODO: create a new collection with a unique name, confirm it exists
        # TODO: delete the collection, make sure it's gone


    def testChangedCollections(self):
        c = self.wrapper.tree_collections
        c.pull()  # get the full git history
        # check for known changed collections in this repo
        changed = c.get_changed_docs('637bb5a35f861d84c115e5e6c11030d1ecec92e0')
        self.assertEqual(set(), changed)
        changed = c.get_changed_docs('d17e91ae85e829a4dcc0115d5d33bf0dca179247')
        self.assertEqual({u'TestUserB/fungal-trees.json'}, changed)
        changed = c.get_changed_docs('af72fb2cc060936c9afce03495ec0ab662a783f6')
        expected = {u'test-user-a/my-favorite-trees.json', u'TestUserB/fungal-trees.json'}
        self.assertEqual(expected, changed)
        # check a doc that changed
        changed = c.get_changed_docs('af72fb2cc060936c9afce03495ec0ab662a783f6',
                                     [u'TestUserB/fungal-trees.json'])
        self.assertEqual({u'TestUserB/fungal-trees.json'}, changed)
        # check a doc that didn't change
        changed = c.get_changed_docs('d17e91ae85e829a4dcc0115d5d33bf0dca179247',
                                     [u'test-user-a/my-favorite-trees.json'])
        self.assertEqual(set(), changed)
        # check a bogus doc id should work, but find nothing
        changed = c.get_changed_docs('d17e91ae85e829a4dcc0115d5d33bf0dca179247',
                                     [u'bogus/fake-trees.json'])
        self.assertEqual(set(), changed)
        # passing a foreign (or nonsense) SHA should raise a ValueError
        self.assertRaises(ValueError, c.get_changed_docs, 'bogus')


if __name__ == "__main__":
    unittest.main(verbosity=5)
