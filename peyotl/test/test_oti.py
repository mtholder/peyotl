#! /usr/bin/env python
from peyotl.api import OTI
from peyotl.test.support.pathmap import get_test_ot_service_domains
from peyotl.utility import get_logger
import sys
import unittest

_LOG = get_logger(__name__)

class TestTaxomachine(unittest.TestCase):
    def setUp(self):
        d = get_test_ot_service_domains()
        self.oti = OTI(d)
    def testStudyTerms(self):
        t_set = self.oti.study_search_term_set
        r = self.oti.find_studies({'ot:studyPublication': '10.1073/pnas.0709121104'})
        print r
class X:
    def testNodeTerms(self):
        t_set = self.oti.node_search_term_set
        qd = {'ot:originalLabel': 'Aponogeoton ulvaceus 1 2'}
        nl = self.oti.find_nodes(qd)
        self.assertTrue(len(nl) > 0)
        f = nl[0]
        self.assertTrue('matched_trees' in f)
        t = f['matched_trees']
        self.assertTrue(len(t) > 0)
        tr = t[0]
        self.assertTrue('matched_nodes' in tr)
        n = tr['matched_nodes']
        self.assertTrue(len(n) > 0)
    def testBadNodeTerms(self):
        qd = {'bogus key': 'Aponogeoton ulvaceus 1 2'}
        self.assertRaises(ValueError, self.oti.find_nodes, qd)
    def testTreeTerms(self):
        t_set = self.oti.tree_search_term_set
        x = list(t_set)
        x.sort()
        print 'tree\n' + '\n'.join(x)
        '''qd = {'ot:originalLabel': 'Aponogeoton ulvaceus 1 2'}
        nl = self.oti.find_nodes(qd)
        self.assertTrue(len(nl) > 0)
        f = nl[0]
        self.assertTrue('matched_trees' in f)
        t = f['matched_trees']
        self.assertTrue(len(t) > 0)
        tr = t[0]
        self.assertTrue('matched_nodes' in tr)
        n = tr['matched_nodes']
        self.assertTrue(len(n) > 0)'''
    def testBadNodeTerms(self):
        qd = {'bogus key': 'Aponogeoton ulvaceus 1 2'}
        self.assertRaises(ValueError, self.oti.find_trees, qd)
if __name__ == "__main__":
    unittest.main()
