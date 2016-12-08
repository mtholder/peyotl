#! /usr/bin/env python
from peyotl.utility import get_logger
from peyotl.nexson_syntax import write_as_json
from peyotl.struct_diff import DictDiff

_LOG = get_logger(__name__)


def equal_blob_check(unit_test, diff_file_tag, first, second):
    from peyotl.test.support import pathmap
    if first != second:
        dd = DictDiff.create(first, second)
        ofn = pathmap.next_unique_scratch_filepath(diff_file_tag + '.obtained_rt')
        efn = pathmap.next_unique_scratch_filepath(diff_file_tag + '.expected_rt')
        write_as_json(first, ofn)
        write_as_json(second, efn)
        er = dd.edits_expr()
        _LOG.info('\ndict diff: {d}'.format(d='\n'.join(er)))
        if first != second:
            m_fmt = "TreeBase conversion failed see files {o} and {e}"
            m = m_fmt.format(o=ofn, e=efn)
            unit_test.assertEqual("", m)


example_ott_id_list = [515698, 515712, 149491, 876340, 505091, 840022, 692350, 451182, 301424, 876348, 515698, 1045579,
                       267484, 128308, 380453, 678579, 883864, 5537065,
                       3898562, 5507605, 673540, 122251, 5507740, 1084532, 541659]


def test_phylesystem_api_for_study(test_case_instance, phylesystem_wrapper, study_id='pg_10'):
    from peyotl.nexson_syntax.helper import detect_nexson_version, find_val_literal_meta_first
    x = phylesystem_wrapper.get(study_id)['data']
    sid = find_val_literal_meta_first(x['nexml'], 'ot:studyId', detect_nexson_version(x))
    test_case_instance.assertTrue(sid in [study_id])
    y = phylesystem_wrapper.get(study_id, tree_id='tree3', format='newick')
    test_case_instance.assertTrue(y.startswith('('))


def test_amendments_api(test_case_instance, amendments_wrapper):
    try:
        a = amendments_wrapper.get('additions-5000000-5000003')
        cn = a['study_id']
        test_case_instance.assertTrue(cn in [u'ot_234', ])
    except:
        # try alternate amendments (and study_id) for remote/proxied docstores
        try:
            # this is an amendment in the production repo!
            a = amendments_wrapper.get('additions-5861452-5861452')
            cn = a['study_id']
            test_case_instance.assertTrue(cn in [u'ot_520', ])
        except:
            # this is an amendment in the devapi repo (amendments-0)!
            a = amendments_wrapper.get('additions-10000000-10000001')
            cn = a['study_id']
            test_case_instance.assertTrue(cn in [u'pg_2606', ])


def test_collections_api(test_case_instance, collections_wrapper):
    try:
        c = collections_wrapper.get('TestUserB/my-favorite-trees')
    except:
        # alternate collection for remote/proxied docstore
        c = collections_wrapper.get('jimallman/my-test-collection')
    cn = c['name']
    test_case_instance.assertTrue(cn in [u'My favorite trees!', u'My test collection'])


def raise_http_error_with_more_detail(err):
    # show more useful information (JSON payload) from the server
    details = err.response.text
    raise ValueError("{e}, details: {m}".format(e=err, m=details))


def test_tol_about(self, cdict):
    for key in [u'date',
                u'num_source_studies',
                u'root_taxon_name',
                u'study_list',
                u'root_ott_id',
                u'root_node_id',
                u'tree_id',
                u'taxonomy_version',
                u'num_tips']:
        self.assertTrue(key in cdict)
    tree_id = cdict['tree_id']
    node_id = str(cdict['root_node_id'])  # Odd that this is a string
    return tree_id, node_id

tests_expected_coll_list = ['TestUserB/fungal-trees', 'TestUserB/my-favorite-trees',
                      'josephwb/hypocreales',
                      'kcranston/barnacles',
                      'mwestneat/reef-fishes',
                      'opentreeoflife/default',
                      'opentreeoflife/fungi',
                      'opentreeoflife/metazoa',
                      'opentreeoflife/plants',
                      'opentreeoflife/safe-microbes',
                      'pcart/cnidaria',
                      'test-user-a/my-favorite-trees', 'test-user-a/trees-about-bees']
tests_expected_coll_list.sort()
tests_expected_change_set = {'TestUserB/fungal-trees', 'josephwb/hypocreales',
                      'kcranston/barnacles',
                      'mwestneat/reef-fishes',
                      'opentreeoflife/default',
                      'opentreeoflife/fungi',
                      'opentreeoflife/metazoa',
                      'opentreeoflife/plants',
                      'opentreeoflife/safe-microbes',
                      'pcart/cnidaria', }


def test_collection_indexing(unittestcase, c):
    k = list(c.get_doc_ids())
    k.sort()
    unittestcase.assertEqual(k, tests_expected_coll_list)
    k = list(c._doc2shard_map.keys())
    k.sort()
    unittestcase.assertEqual(k, tests_expected_coll_list)

def test_changed_collections(unittestcase, c):
    c.pull()  # get the full git history
    # check for known changed collections in this repo
    changed = c.get_changed_docs('637bb5a35f861d84c115e5e6c11030d1ecec92e0')
    unittestcase.assertEqual(tests_expected_change_set, changed)
    changed = c.get_changed_docs('d17e91ae85e829a4dcc0115d5d33bf0dca179247')
    unittestcase.assertEqual(tests_expected_change_set, changed)
    changed = c.get_changed_docs('af72fb2cc060936c9afce03495ec0ab662a783f6')
    expected = {u'test-user-a/my-favorite-trees'}
    expected.update(tests_expected_change_set)
    unittestcase.assertEqual(expected, changed)
    # check a doc that changed
    changed = c.get_changed_docs('af72fb2cc060936c9afce03495ec0ab662a783f6',
                                 [u'TestUserB/fungal-trees'])
    unittestcase.assertEqual({u'TestUserB/fungal-trees'}, changed)
    # check a doc that didn't change
    changed = c.get_changed_docs('d17e91ae85e829a4dcc0115d5d33bf0dca179247',
                                 [u'test-user-a/my-favorite-trees'])
    unittestcase.assertEqual(set(), changed)
    # check a bogus doc id should work, but find nothing
    changed = c.get_changed_docs('d17e91ae85e829a4dcc0115d5d33bf0dca179247',
                                 [u'bogus/fake-trees'])
    unittestcase.assertEqual(set(), changed)
    # passing a foreign (or nonsense) SHA should raise a ValueError
    unittestcase.assertRaises(ValueError, c.get_changed_docs, 'bogus')
