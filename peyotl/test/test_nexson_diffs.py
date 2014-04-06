#! /usr/bin/env python
from peyotl.nexson_diff import NexsonDiff
from peyotl.nexson_syntax import write_as_json
from peyotl.test.support import pathmap
from peyotl.utility import get_logger
import unittest
import shutil
import codecs
import json
import os
_LOG = get_logger(__name__)

def emulate_conflicted_merge(mrca_file,
                             user_version,
                             other_version, 
                             study_filepath):
    shutil.copy(other_version, study_filepath)
    edits_on_dest = NexsonDiff(mrca_file, user_version)
    edits_on_dest.patch_modified_file(study_filepath)
    diffs_from_dest_par = NexsonDiff(user_version, study_filepath)
    return edits_on_dest, diffs_from_dest_par

def read_json(fp):
    return json.load(codecs.open(fp, 'rU', encoding='utf-8'))


class TestNexsonDiff(unittest.TestCase):

    def testExpectedMerge(self):
        for fn in pathmap.all_dirs(os.path.join('nexson', 'diff')):
            mrca_file = os.path.join(fn, 'mrca.json')
            user_version = os.path.join(fn, 'by-user.json')
            other_version = os.path.join(fn, 'by-others.json')
            output = os.path.join(fn, 'output')
            eod, dfdp = emulate_conflicted_merge(mrca_file, 
                                                 user_version,
                                                 other_version,
                                                 output)
            expected = os.path.join(fn, 'expected-output.json')
            expected_blob = read_json(expected)
            output_blob = read_json(output)
            e = eod.unapplied_edits_as_ot_diff_dict()
            d = dfdp.as_ot_diff_dict()
            u = os.path.join(fn, 'unapplied.json')
            eu = os.path.join(fn, 'expected-unapplied.json')
            df = os.path.join(fn, 'diff-from-user.json')
            edf = os.path.join(fn, 'expected-diff-from-user.json')
            write_as_json(e, u)
            write_as_json(d, df)
            exp_e = read_json(eu)
            exp_d = read_json(edf)
            self.assertDictEqual(expected_blob, output_blob, "Patch failed to produce expected outcome. Compare {o} and {e}".format(o=output, e=expected))
            self.assertDictEqual(exp_e, e, "Patch failed to produce expected unapplied. Compare {o} and {e}".format(o=u, e=eu))
            self.assertDictEqual(exp_d, d, "Patch failed to produce expected diff. Compare {o} and {e}".format(o=df, e=edf))
if __name__ == "__main__":
    unittest.main()
