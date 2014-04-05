#!/usr/bin/env python
from peyotl.nexson_syntax import detect_nexson_version, \
                                 read_as_json, \
                                 write_as_json, \
                                 _is_by_id_hbf
from peyotl.struct_diff import DictDiff, ListDiff
import itertools
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

def _list_patch_modified_blob(nexson_diff, base_blob, dels, adds, mods):
        for t in mods:
            nexson_diff._unapplied_edits['modifications'].append(t)
        for t in self._deletions:
            nexson_diff._unapplied_edits['deletions'].append(t)
        for t in self._additions:
            nexson_diff._unapplied_edits['additions'].append(t)

def _dict_patch_modified_blob(nexson_diff, base_blob, dels, adds, mods):
    for t in dels:
        k, v, c = t
        if k in base_blob:
            del base_blob[k]
        else:
            nexson_diff._redundant_edits['deletions'].append((k, c))
    adds_to_mods = []
    really_adds = set()
    for t in adds:
        k, v, c = t
        if k in base_blob:
            t = (k, v, c)
            really_adds.add(t)
            adds_to_mods.append(t)
        elif isinstance(v, DictDiff) or isinstance(v, ListDiff):
            nexson_diff._unapplied_edits['additions'].append(t)
        else:
            base_blob[k] = v
    mods_to_adds = []
    for t in itertools.chain(mods, adds_to_mods):
        k, v, c = t
        if k not in base_blob:
            mods_to_adds.append((k, v, c))
        else:
            if isinstance(v, DictDiff):
                _dict_patch_modified_blob(nexson_diff, base_blob[k], v._deletions, v._additions, v._modifications)
            elif isinstance(v, ListDiff):
                _list_patch_modified_blob(nexson_diff, base_blob[k], v._deletions, v._additions, v._modifications)
            else:
                if base_blob.get(k) == v:
                    if t in really_adds:
                        container = nexson_diff._redundant_edits['additions']
                    else:
                        container = nexson_diff._redundant_edits['modifications']
                    container.append(t)
                else:
                    base_blob[k] = v
    for t in adds:
        k, v, c = t
        if isinstance(v, DictDiff) or isinstance(v, ListDiff):
            nexson_diff._unapplied_edits['modifications'].append(t)
        else:
            base_blob[k] = v

OT_DIFF_TYPE_LIST = ('additions', 'deletions', 'modifications')

def _to_ot_diff_dict(native_diff):
    r = {}
    for dt in OT_DIFF_TYPE_LIST:
        kvc_list = native_diff.get(dt, [])
        x = []
        for k, v, c in kvc_list:
            y = {'key':k, 'value': v}
            if c is not None:
                y['refersTo'] = c.as_ot_target()
            x.append(y)
        if kvc_list:
            r[dt] = x
    return r

class NexsonDiff(DictDiff):
    def __init__(self, anc, des):
        DictDiff.__init__(self)
        self.anc_blob = _get_blob(anc)
        self.des_blob = _get_blob(des)
        self._unapplied_edits = {}
        self._redundant_edits = {}
        self._calculate_diff()
        self._diff = None

    def patch_modified_file(self, filepath_to_patch):
        assert(isinstance(filepath_to_patch, str) or isinstance(filepath_to_patch, unicode))
        base_blob = _get_blob(filepath_to_patch)
        self.patch_modified_blob(base_blob)
        write_as_json(base_blob, filepath_to_patch)

    def get_diff_dict(self):
        if self._diff is None:
            self._diff = { 'additions': self._additions,
                           'deletions': self._deletions,
                           'modifications': self._modifications,
            }
        return self._diff
    diff_dict = property(get_diff_dict)
    def unapplied_edits_as_ot_diff_dict(self):
        if self._unapplied_edits is None:
            return {}
        return _to_ot_diff_dict(self._unapplied_edits)

    def as_ot_diff_dict(self):
        return _to_ot_diff_dict(self.diff_dict)

    def patch_modified_blob(self, base_blob):
        self._unapplied_edits = {}
        self._redundant_edits = {}
        for dt in OT_DIFF_TYPE_LIST:
            self._unapplied_edits[dt] = []
            self._redundant_edits[dt] = []
        _dict_patch_modified_blob(self, base_blob, self._deletions, self._additions, self._modifications)

    def _calculate_diff(self):
        '''Inefficient comparison of anc and des dicts.
        Recurses through dict and lists.
        
        '''
        self._additions = []
        self._deletions = []
        self._modifications = []
        self._diff = None
        a = self.anc_blob
        d = self.des_blob
        anc_nexml = a['nexml']
        des_nexml = d['nexml']
        anc_otus = anc_nexml['otusById']
        des_otus = des_nexml['otusById']
        anc_trees = anc_nexml['treesById']
        des_trees = des_nexml['treesById']

        otu_diff = (self._handle_otu_diffs, None)
        otus_diff = (self._handle_otus_diffs, {'otuById': otu_diff})
        edge_diff = (self._handle_edge_diffs, None)
        node_diff = (self._handle_node_diffs, None)
        tree_diff = (self._handle_tree_diffs, {'edgeBySourceId': edge_diff, 'nodeById': node_diff})
        trees_diff = (self._handle_trees_diffs, {'treeById': tree_diff})
        nexml_diff = (self._handle_nexml_diffs, { 'otusById': otus_diff, 'treesById': trees_diff})
        skip = {'nexml': nexml_diff}
        self._calculate_generic_diffs(a, d, skip)
        self._unapplied_edits = None

    def _handle_nexml_diffs(self, src, dest, skip_dict, context):
        return True, context
    def _handle_otu_diffs(self, src, dest, skip_dict, context):
        return True, context
    def _handle_otus_diffs(self, src, dest, skip_dict, context):
        return True, context
    def _handle_edge_diffs(self, src, dest, skip_dict, context):
        return True, context
    def _handle_node_diffs(self, src, dest, skip_dict, context):
        return True, context
    def _handle_tree_diffs(self, src, dest, skip_dict, context):
        return True, context
    def _handle_trees_diffs(self, src, dest, skip_dict, context):
        return True, context
    def _handle_trees_diffs(self, src, dest, skip_dict, context):
        return True, context
    def add_addition(self, k, v, context=None):
        self._additions.append((k, v, context))
    def add_deletion(self, k, v, context=None):
        self._deletions.append((k, v, context))
    def add_modification(self, k, v, context=None):
        self._modifications.append((k, v, context))

    def _calculate_generic_diffs(self, src, dest, skip_dict=None, context=None):
        sk = set(src.keys())
        dk = set(dest.keys())
        if skip_dict is None:
            skip_dict = {}
        for k in sk:
            do_generic_calc = True
            v = src[k]
            skip_tuple = skip_dict.get(k)
            sub_skip_dict = None
            sub_context = context
            if skip_tuple is not None:
                func, sub_skip_dict = skip_tuple
                do_generic_calc, sub_context = func(v, dest.get(k), sub_skip_dict, context=sub_context)
            if do_generic_calc:
                if k in dk:
                    dv = dest[k]
                    if v != dv:
                        rec_call = None
                        if isinstance(v, dict) and isinstance(dv, dict):
                            rec_call = self._calculate_generic_diffs(v, dv, skip_dict=sub_skip_dict, context=sub_context)
                        elif isinstance(v, list) and isinstance(dv, list):
                            rec_call = ListDiff.create(v, dv, wrap_dict_in_list=True)
                        else:
                            if isinstance(v, dict) and isinstance(dv, list):
                                rec_call = ListDiff.create([v], dv, wrap_dict_in_list=True)
                            elif isinstance(dv, dict) or isinstance(v, list):
                                rec_call = ListDiff.create(v, [dv], wrap_dict_in_list=True)
                        if rec_call is not None:
                            self.add_modification(k, rec_call, context=sub_context)
                        else:
                            self.add_modification(k, dv, context=sub_context)
                else:
                    self.add_deletion(k, v, context=sub_context)
        add_keys = dk - sk
        for k in add_keys:
            do_generic_calc = True
            skip_tuple = skip_dict.get(k)
            sub_skip_dict = None
            sub_context = context
            if skip_tuple is not None:
                func, sub_skip_dict = skip_tuple
                do_generic_calc, sub_context = func(v, dest.get(k), sub_skip_dict)
            if do_generic_calc:
                self.add_addition(k, dest[k], context=sub_context)
        self.finish()
        return self
