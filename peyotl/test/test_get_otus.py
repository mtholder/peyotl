#! /usr/bin/env python
from peyotl.nexson_syntax import sort_arbitrarily_ordered_nexson, get_otu_mapping
from peyotl.manip import merge_otus_and_trees
from peyotl.test.support import pathmap
from peyotl.test.support import equal_blob_check
from peyotl.utility import get_logger
import unittest

_LOG = get_logger(__name__)


class TestGetOtus(unittest.TestCase):
    def testCanGet(self):
        inp = pathmap.nexson_obj('10/pg_10.json')
        expected = pathmap.nexson_obj('otu/mapped_otus.json')
        self.assertNotEqual(inp, expected)
        map_dict = get_otu_mapping(inp)
        self.assertEqual(set(expected.keys()), set(map_dict.keys()))
        for otu in map_dict:
            self.assertEqual(set(map_dict[otu]), set(expected[otu]))


if __name__ == "__main__":
    unittest.main()