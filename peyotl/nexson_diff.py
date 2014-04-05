#!/usr/bin/env python
from peyotl.nexson_syntax import detect_nexson_version, \
                                 read_as_json, \
                                 write_as_json, \
                                 _is_by_id_hbf
from peyotl.struct_diff import DictDiff, ListDiff
from peyotl.utility import get_logger
import itertools
import json
_LOG = get_logger(__name__)
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
    for v, c in mods:
        nexson_diff._unapplied_edits['modifications'].append((v, c))
    for c in self._deletions:
        nexson_diff._unapplied_edits['deletions'].append(c)
    for v, c in self._additions:
        nexson_diff._unapplied_edits['additions'].append((v, c))
    return True

def _dict_patch_modified_blob(nexson_diff, base_blob, dels, adds, mods):
    for c in dels:
        c.try_apply_del_to_mod_blob(nexson_diff, base_blob)
    adds_to_mods = []
    really_adds = set()
    for t in adds:
        v, c = t
        added, is_mod = c.try_apply_add_to_mod_blob(nexson_diff, base_blob, v, False)
        if not added:
            if is_mod:
                really_adds.add(t)
                adds_to_mods.append(t)
            else:
                nexson_diff._unapplied_edits['additions'].append(t)
    mods_to_adds = []
    for t in itertools.chain(mods, adds_to_mods):
        v, c = t
        was_add = t in really_adds
        if not c.try_apply_mod_to_mod_blob(nexson_diff, base_blob, v, was_add):
            mods_to_adds.append(t)
    for t in mods_to_adds:
        v, c = t
        added, is_mod = c.try_apply_add_to_mod_blob(nexson_diff, base_blob, v, True)
        if not added:
            nexson_diff._unapplied_edits['additions'].append(t)

OT_DIFF_TYPE_LIST = ('additions', 'deletions', 'modifications')

def _to_ot_diff_dict(native_diff):
    r = {}
    for dt in ('additions', 'modifications'):
        kvc_list = native_diff.get(dt, [])
        x = []
        for v, c in kvc_list:
            y = {'value': v}
            if c is not None:
                y['refersTo'] = c.as_ot_target()
            x.append(y)
        if kvc_list:
            r[dt] = x

    dt = 'deletions'
    kvc_list = native_diff.get(dt, [])
    x = []
    for c in kvc_list:
        y = {}
        if c is not None:
            y['refersTo'] = c.as_ot_target()
        x.append(y)
    if kvc_list:
        r[dt] = x
    return r

class NexsonContext(object):
    def __init__(self, par=None, key_in_par=None):
        self.par = par
        self.key_in_par = key_in_par
        self._as_ot_dict = None
        self._mb_cache = {}
    def child(self, key_in_par):
        return NexsonContext(par=self, key_in_par=key_in_par)
    def as_ot_target(self):
        if self._as_ot_dict is None:
            if self.par is None:
                if self.key_in_par is None:
                    self._as_ot_dict = {'path': tuple()}
                else:
                    self._as_ot_dict = {'path': (self.key_in_par,)}
            else:
                assert(self.key_in_par is not None)
                pl = [i for i in self.par.as_ot_target()['path']]
                pl.append(self.key_in_par)
                self._as_ot_dict = {'path': tuple(pl)}
        return self._as_ot_dict

    def _find_par_el_in_mod_blob(self, blob):
        return self.par._find_el_in_mod_blob(blob)

    def _find_el_in_mod_blob(self, blob):
        if self.par is None:
            return blob
        ib = id(blob)
        target = self._mb_cache.get(ib)
        if target is None:
            if self._mb_cache:
                self._mb_cache = {}
            par_target = self._find_par_el_in_mod_blob(blob)
            assert(isinstance(par_target, dict))
            target = par_target.get(self.key_in_par)
            if target is not None:
               self._mb_cache[ib] = target
        return target


    def try_apply_del_to_mod_blob(self, nexson_diff, blob):
        assert(self.par is not None) # can't delete the whole blob!
        par_target = self.par._find_par_el_in_mod_blob(blob)
        if par_target is None:
            return False
        assert(isinstance(par_target, dict))
        if self.key_in_par in par_target:
            del target[self.key_in_par]
            self._mb_cache = {}
            return True
        nexson_diff._redundant_edits['deletions'].append(self)
        return False

    def try_apply_add_to_mod_blob(self, nexson_diff, blob, value, was_mod):
        '''Returns ("value in blob", "presence of key makes this addition a modification")
        records _redundant_edits 
        was_mod should be true if the diff was originally a modification
        '''
        assert(self.par is not None)
        par_target = self._find_par_el_in_mod_blob(blob)
        if par_target is None:
            return False, False
        assert(isinstance(par_target, dict))
        if self.key_in_par in par_target:
            if par_target[self.key_in_par] == value:
                if was_mod:
                    container = nexson_diff._redundant_edits['modifications']
                else:
                    container = nexson_diff._redundant_edits['additions']
                container.append(self)
                return True, True
            return False, True
        assert(not isinstance(value, DictDiff))
        assert(not isinstance(value, ListDiff))
        par_target[self.key_in_par] = value
        self._mb_cache = {id(blob): par_target[self.key_in_par]}
        return True, True

    def try_apply_mod_to_mod_blob(self, nexson_diff, blob, value, was_add):
        par_target = self._find_par_el_in_mod_blob(blob)
        if self.key_in_par not in par_target:
            return False
        #_LOG.debug('try_apply_mod_to_mod_blob par_target.keys() = {} self.key_in_par = "{}"'.format(par_target.keys(), self.key_in_par))
        assert(not isinstance(value, DictDiff))
        if isinstance(value, ListDiff):
            target = self._find_el_in_mod_blob(blob)
            if not isinstance(target, list):
                target = [target]
                par_target[self.key_in_par] = target
            return _list_patch_modified_blob(nexson_diff, target, value._deletions, value._additions, value._modifications)
        if par_target.get(self.key_in_par) == value:
            if t in really_adds:
                container = nexson_diff._redundant_edits['additions']
            else:
                container = nexson_diff._redundant_edits['modifications']
            container.append(t)
        else:
            par_target[self.key_in_par] = value
        return True


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
        self._retained_otus_id_set = set()
        self._added_otus_id_map= {}
        self._del_otus_id_set = set()
        self._dest_otus_order = []

        otu_diff = (self._normal_handling, None)
        otus_diff = (self._handle_otus_diffs, {'otuById': otu_diff})
        edge_diff = (self._handle_edge_diffs, None)
        node_diff = (self._handle_node_diffs, None)
        tree_diff = (self._handle_tree_diffs, {'edgeBySourceId': edge_diff, 'nodeById': node_diff})
        trees_diff = (self._handle_trees_diffs, {'treeById': tree_diff})
        nexml_diff = (self._handle_nexml_diffs, {'otusById': otus_diff,
                                                 '^ot:otusElementOrder': (self._no_op_handling, None), 
                                                 'treesById': trees_diff})
        skip = {'nexml': nexml_diff}
        context = NexsonContext()
        self._calculate_generic_diffs(a, d, skip, context)
        self._unapplied_edits = None

    def _no_op_handling(self, src, dest, skip_dict, context, key_in_par):
        return False, None
    def _handle_nexml_diffs(self, src, dest, skip_dict, context, key_in_par):
        if (dest is None) or (src is None):
            return True, None
        sub_context = context.child(key_in_par)
        src_otus_order = src.get('^ot:otusElementOrder', [])
        src_otus_set = set(src_otus_order)
        self._dest_otus_order = dest.get('^ot:otusElementOrder', [])
        dest_otus_set = set(self._dest_otus_order)
        dest_otus = dest.get('otusById', {})
        for o in dest_otus_set:
            if o in src_otus_set:
                self._retained_otus_id_set.add(o)
            else:
                self._added_otus_id_map[o] = dest_otus[0]
        for o in src_otus_set:
            if not (o in self. _dest_otus_order):
                self._del_otus_id_set.add(o)
        self._calculate_generic_diffs(src, dest, skip_dict, sub_context)
        return False, None

        return True, None
    def _normal_handling(self, src, dest, skip_dict, context, key_in_par):
        return True, None
    def _handle_otus_diffs(self, src, dest, skip_dict, context, key_in_par):
        return True, None
    def _handle_edge_diffs(self, src, dest, skip_dict, context, key_in_par):
        return True, None
    def _handle_node_diffs(self, src, dest, skip_dict, context, key_in_par):
        return True, None
    def _handle_tree_diffs(self, src, dest, skip_dict, context, key_in_par):
        return True, None
    def _handle_trees_diffs(self, src, dest, skip_dict, context, key_in_par):
        return True, None
    def _handle_trees_diffs(self, src, dest, skip_dict, context, key_in_par):
        return True, None
    def add_addition(self, v, context):
        self._additions.append((v, context))
    def add_deletion(self, context):
        self._deletions.append(context)
    def add_modification(self, v, context):
        self._modifications.append((v, context))

    def _calculate_generic_diffs(self, src, dest, skip_dict, context):
        sk = set(src.keys())
        dk = set(dest.keys())
        if skip_dict is None:
            skip_dict = {}
        for k in sk:
            do_generic_calc = True
            v = src[k]
            skip_tuple = skip_dict.get(k)
            sub_skip_dict = None
            sub_context = None
            if skip_tuple is not None:
                func, sub_skip_dict = skip_tuple
                do_generic_calc, sub_context = func(v, dest.get(k), sub_skip_dict, context=context, key_in_par=k)
            if do_generic_calc:
                if k in dk:
                    dv = dest[k]
                    if v != dv:
                        rec_call = None
                        if isinstance(v, dict) and isinstance(dv, dict):
                            if sub_context is None:
                                sub_context = context.child(k)
                            self._calculate_generic_diffs(v, dv, skip_dict=sub_skip_dict, context=sub_context)
                        else:
                            if isinstance(v, list) and isinstance(dv, list):
                                rec_call = ListDiff.create(v, dv, wrap_dict_in_list=True)
                            else:
                                if isinstance(v, dict) and isinstance(dv, list):
                                    rec_call = ListDiff.create([v], dv, wrap_dict_in_list=True)
                                elif isinstance(dv, dict) or isinstance(v, list):
                                    rec_call = ListDiff.create(v, [dv], wrap_dict_in_list=True)
                            if sub_context is None:
                                sub_context = context.child(k)
                            if rec_call is not None:
                                self.add_modification(rec_call, context=sub_context)
                            else:
                                self.add_modification(dv, context=sub_context)
                else:
                    if sub_context is None:
                        sub_context = context.child(k)
                    self.add_deletion(context=sub_context)
        add_keys = dk - sk
        for k in add_keys:
            do_generic_calc = True
            skip_tuple = skip_dict.get(k)
            sub_skip_dict = None
            if skip_tuple is not None:
                func, sub_skip_dict = skip_tuple
                do_generic_calc, sub_context = func(v, dest.get(k), sub_skip_dict, context=context, key_in_par=k)
            if do_generic_calc:
                if sub_context is None:
                    sub_context = context.child(k)
                self.add_addition(dest[k], context=sub_context)
        self.finish()
        return self
