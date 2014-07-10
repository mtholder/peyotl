#! /usr/bin/env python
from peyotl.nexson_diff import NexsonDiff
from peyotl.nexson_syntax import write_as_json
from peyotl.test.support import pathmap
from peyotl.utility import get_logger
import unittest
import shutil
import codecs
import json
import sys
import os
_LOG = get_logger(__name__)

def emulate_conflicted_merge(mrca_file,
                             user_version,
                             other_version, 
                             study_filepath):
    base = study_filepath + '.tmp'
    shutil.copy(other_version, base)
    edits_on_dest = NexsonDiff(mrca_file, user_version)
    #write_as_json(edits_on_dest.as_ot_diff_dict(), sys.stderr)
    edits_on_dest.patch_modified_file(base, output_filepath=study_filepath)
    os.remove(base)
    diffs_from_dest_par = NexsonDiff(user_version, study_filepath)
    return edits_on_dest, diffs_from_dest_par

def read_json(fp):
    return json.load(codecs.open(fp, 'rU', encoding='utf-8'))

def rec_dict_diff(f, t, p):
    for k, v in f.items():
        v2 = t.get(k)
        if v2 != v:
            if isinstance(v, dict) and isinstance(v2, dict):
                rec_dict_diff(v, v2, p + '/' + k)
            else:
                _LOG.debug('expected {p}/{k} = "{v}"'.format(p=p, k=k, v=v))
                _LOG.debug('obtained {p}/{k} = "{v}"'.format(p=p, k=k, v=v2))

class TestNexsonDiff(unittest.TestCase):

    def testExpectedMerge(self):
        for fn in pathmap.all_dirs(os.path.join('nexson', 'diff')):
            if not fn.endswith('bare-mod-no-mod-list'):
                pass
            mrca_file = os.path.join(fn, 'mrca.json')
            user_version = os.path.join(fn, 'by-user.json')
            other_version = os.path.join(fn, 'by-others.json')
            output = os.path.join(fn, 'output')
            eod, dfdp = emulate_conflicted_merge(mrca_file, 
                                                 user_version,
                                                 other_version,
                                                 output)
            expected = os.path.join(fn, 'expected-output.json')
            #import time; time.sleep()
            #_LOG.debug('reading expected_blob from ' + expected)
            expected_blob = read_json(expected)
            #_LOG.debug('reading output_blob from ' + output)
            output_blob = read_json(output)
            rec_dict_diff(expected_blob, output_blob, '')
            e = eod.unapplied_edits_as_ot_diff_dict()
            e = json.loads(json.dumps(e, encoding='utf-8'), encoding='utf-8')
            x = dfdp.as_ot_diff_dict()
            d = json.loads(json.dumps(x, encoding='utf-8'), encoding='utf-8')
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
            rec_dict_diff(exp_d, d, '')
            self.assertDictEqual(exp_d, d, "Patch failed to produce expected diff. Compare {o} and {e}".format(o=df, e=edf))
if __name__ == "__main__":
    unittest.main()
