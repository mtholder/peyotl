#!/usr/bin/env python
from peyotl.nexson_diff.helper import extract_by_diff_type, \
                                      new_diff_summary, \
                                      to_ot_diff_dict
class PatchReason:
    SUCCESS, CONTAINER_GONE, EDITED, REDUNDANT, TOO_DIFFERENT, ELEMENT_GONE = range(6)

class PatchLog(object):
    def __init__(self):
        self.unapplied_edits = new_diff_summary()
        self.redundant_edits = new_diff_summary()
    def mark_unapplied(self, d_obj, reason):
        extract_by_diff_type(self.unapplied_edits, d_obj).append(d_obj)
    def mark_redundant(self, d_obj, reason):
        extract_by_diff_type(self.redundant_edits, d_obj).append(d_obj)
    def unapplied_as_ot_diff_dict(self):
        return to_ot_diff_dict(self.unapplied_edits)
