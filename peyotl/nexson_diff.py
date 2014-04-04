#!/usr/bin/env python
from peyotl.nexson_syntax import detect_nexson_version, \
                                 read_as_json, \
                                 write_as_json, \
                                 _is_by_id_hbf
import json
def _get_blob(src):
    if isinstance(src, str) or isinstance(src, unicode):
        b = read_as_json(src)
    elif isinstance(src, dict):
        b = src
    else:
        b = json.load(src)
    v = detect_nexson_version(b)
    if not _is_by_id_hbf(v):
        raise ValueError('NexsonDiff objects can only operate on NexSON version 1.2. Found version = "{}"'.format(v))
    return b
def _to_ot_diff_dict(native_diff):
    raise NotImplementedError()

class NexsonDiff(object):
    def __init__(self, anc, des):
        self.anc_blob = _get_blob(anc)
        self.des_blob = _get_blob(des)
        self._unapplied_edits = {}
        self._diffs = {}

    def patch_modified_file(self, filepath_to_patch):
        assert(isinstance(filepath_to_patch, str) or isinstance(filepath_to_patch, unicode))
        base_blob = _get_blob(filepath_to_patch)
        self.patch_modified_blob(self, base_blob)
        write_as_json(base_blob, filepath_to_patch)

    def unapplied_edits_as_ot_diff_dict(self):
        return _to_ot_diff_dict(self._unapplied_edits)

    def as_ot_diff_dict(self):
        return _to_ot_diff_dict(self._diffs)

    def patch_modified_blob(self, base_blob):
        self._unapplied_edits = {}
        
        raise NotImplementedError()