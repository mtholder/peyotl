#!/usr/bin/env python
from peyotl.nexson_syntax import detect_nexson_version, \
                                 read_as_json, \
                                 write_as_json, \
                                 _is_by_id_hbf, \
                                 edge_by_source_to_edge_dict
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

def new_diff_summary(in_tree=False):
    d = {'additions': [],
            'deletions': [],
            'modifications': [],
    }
    if in_tree:
        d['rerootings'] = []
    return d


def _list_patch_modified_blob(nexson_diff, base_blob, dels, adds, mods):
    for v, c in mods:
        nexson_diff._unapplied_edits['modifications'].append((v, c))
    for c in dels:
        nexson_diff._unapplied_edits['deletions'].append(c)
    for v, c in adds:
        nexson_diff._unapplied_edits['additions'].append((v, c))
    return True

def _dict_patch_modified_blob(nexson_diff, base_blob, diff_dict):
    dels = diff_dict['deletions']
    adds = diff_dict['additions']
    mods = diff_dict['modifications']
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

_tree_dict_patch_modified_blob = _dict_patch_modified_blob

OT_DIFF_TYPE_LIST = ('additions', 'deletions', 'modifications')

def _to_ot_diff_dict(native_diff):
    r = {}
    for dt in ('additions', 'modifications'):
        kvc_list = native_diff.get(dt, [])
        x = []
        for v, c in kvc_list:
            y = {'value': v}
            if c is not None:
                y.update(c.as_ot_target())
            x.append(y)
        if kvc_list:
            r[dt] = x

    dt = 'deletions'
    kvc_list = native_diff.get(dt, [])
    x = []
    for c in kvc_list:
        y = {}
        if c is not None:
            y.update(c.as_ot_target())
        x.append(y)
    if kvc_list:
        r[dt] = x
    return r

def _process_order_list_and_dict(order_key, by_id_key, src, dest):
    src_order = src.get(order_key, [])
    src_set = set(src_order)
    dest_order = dest.get(order_key, [])
    dest_set = set(dest_order)
    dest_otus = dest.get(by_id_key, {})
    ret_id_set = set()
    add_id_map = {}
    del_id_set = set()
    for o in dest_set:
        if o in src_set:
            ret_id_set.add(o)
        else:
            add_id_map[o] = dest_otus[o]
    for o in src_set:
        if not (o in dest_order):
            del_id_set.add(o)
    return {'dest_order': dest_order,
            'retained_id_set': ret_id_set,
            'added_id_map': add_id_map,
            'deleted_id_set': del_id_set}

class NexsonDiffAddress(object):
    def __init__(self, par=None, key_in_par=None):
        self.par = par
        self.key_in_par = key_in_par
        self._as_ot_dict = None
        self._mb_cache = {}
    def child(self, key_in_par):
        return NexsonDiffAddress(par=self, key_in_par=key_in_par)
    def as_ot_target(self):
        if self._as_ot_dict is None:
            if self.par is None:
                if self.key_in_par is None:
                    self._as_ot_dict = {'address': tuple()}
                else:
                    self._as_ot_dict = {'address': (self.key_in_par,)}
            else:
                assert(self.key_in_par is not None)
                pl = [i for i in self.par.as_ot_target()['address']]
                pl.append(self.key_in_par)
                self._as_ot_dict = {'address': tuple(pl)}
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
            del par_target[self.key_in_par]
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
                container.append((value, self))
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
            if was_add:
                container = nexson_diff._redundant_edits['additions']
            else:
                container = nexson_diff._redundant_edits['modifications']
            container.append((value, self))
        else:
            par_target[self.key_in_par] = value
        return True


class NexsonDiff(object):
    def __init__(self, anc, des):
        self.anc_blob = _get_blob(anc)
        self.des_blob = _get_blob(des)
        self.no_op_t = (self._no_op_handling, None)
        self._calculate_diff()
        self._diff = None

    def patch_modified_file(self, filepath_to_patch):
        assert(isinstance(filepath_to_patch, str) or isinstance(filepath_to_patch, unicode))
        base_blob = _get_blob(filepath_to_patch)
        self.patch_modified_blob(base_blob)
        write_as_json(base_blob, filepath_to_patch)

    def get_diff_dict(self)     :
        if self._diff is None:
            self._diff = {'tree':{}}
            for k in OT_DIFF_TYPE_LIST:
                self._diff[k] = self._nontree_diff[k]
                self._diff['tree'][k] = self._tree_diff[k]
            self._diff['tree']['rerootings'] = self._tree_diff['rerootings']
        return self._diff
    diff_dict = property(get_diff_dict)
    def unapplied_edits_as_ot_diff_dict(self):
        if self._unapplied_edits is None:
            return {}
        return _to_ot_diff_dict(self._unapplied_edits)

    def as_ot_diff_dict(self):
        return _to_ot_diff_dict(self.diff_dict)

    def has_differences(self):
        d = self.diff_dict
        t = self.diff_dict.get('tree', {})
        for k in OT_DIFF_TYPE_LIST:
            if d.get(k) or t.get(k):
                return True
        if t.get('rerootings'):
            return True
        return False

    def _clear_patch_related_data(self):
        self._unapplied_nontree_edits = new_diff_summary()
        self._unapplied_tree_edits = new_diff_summary(in_tree=True)
        self._redundant_nontree_edits = new_diff_summary()
        self._redundant_tree_edits = new_diff_summary(in_tree=True)
        self._redundant_edits = self._redundant_nontree_edits
        self._unapplied_edits = self._unapplied_nontree_edits
        
    def _clear_diff_related_data(self):
        self._nontree_diff = new_diff_summary()
        self._tree_diff = new_diff_summary(in_tree=True)
        self.activate_nontree_diffs()
        self._diff = None
        self._retained_otus_id_set = set()
        self._added_otus_id_map = {}
        self._del_otus_id_set = set()
        self._dest_otus_order = []
        # the following is not mutable, so it could go in __init__
        otus_diff = (self._handle_otus_diffs, None)
        trees_diff = (self._handle_trees_diffs, None)
        nexml_diff = (self._handle_nexml_diffs, {'otusById': otus_diff,
                                                 '^ot:otusElementOrder': self.no_op_t, 
                                                 '^ot:treesElementOrder': self.no_op_t, 
                                                 'treesById': trees_diff})
        self.top_skip_dict = {'nexml': nexml_diff}
        
        
    def patch_modified_blob(self, base_blob):
        self._clear_patch_related_data()
        d = self.diff_dict
        _dict_patch_modified_blob(self, base_blob, d)
        self._redundant_edits = self._redundant_tree_edits
        self._unapplied_edits = self._unapplied_tree_edits
        _tree_dict_patch_modified_blob(self, base_blob, d['tree'])

    def _calculate_diff(self):
        '''Inefficient comparison of anc and des dicts.
        Recurses through dict and lists.
        
        '''
        self._clear_patch_related_data()
        self._clear_diff_related_data()
        a = self.anc_blob
        d = self.des_blob
        context = NexsonDiffAddress()
        self._calculate_generic_diffs(a, d, self.top_skip_dict, context)


    def _process_ordering_pair(self,
                               order_key,
                               by_id_key,
                               storage_key,
                               storage_container,
                               src,
                               dest):
        d = {} if (dest is None) else dest
        s = {} if (src is None) else src
        r = _process_order_list_and_dict(order_key, by_id_key, s, d)
        storage_container[storage_key] = r

    def _no_op_handling(self, src, dest, skip_dict, context, key_in_par):
        return False, None

    def _handle_nexml_diffs(self, src, dest, skip_dict, context, key_in_par):
        self._process_ordering_pair('^ot:otusElementOrder', 'otusById', '_otus_order', self.__dict__, src, dest)
        self._process_ordering_pair('^ot:treesElementOrder', 'treesById', '_trees_order', self.__dict__, src, dest)
        return True, None

    def _normal_handling(self, src, dest, skip_dict, context, key_in_par):
        return True, None

    def _handle_otus_diffs(self, src, dest, skip_dict, context, key_in_par):
        sub_context = context.child(key_in_par)
        for otus_id, s_otus in src.items():
            otusid_context = sub_context.child(otus_id)
            d_otus = dest.get(otus_id)
            if d_otus is not None:
                assert(d_otus.keys() == ['otuById'])
                assert(s_otus.keys() == ['otuById'])
                obi_context = otusid_context.child('otuById')
                s_obi = s_otus['otuById']
                d_obi = d_otus['otuById']
                self._calculate_generic_diffs(s_obi, d_obi, None, obi_context)
        return False, None
    
    def _handle_tree_diffs(self, src, dest, skip_dict, context, key_in_par):
        sk_d = {'edgeBySourceId': self.no_op_t,
                'nodeById': self.no_op_t,
                '^ot:rootNodeId': self.no_op_t}
        sub_context = context.child(key_in_par)
        for tree_id, s_tree in src.items():
            tid_context = sub_context.child(tree_id)
            d_tree = dest.get(tree_id)
            if d_tree is not None:
                self._calculate_generic_diffs(s_tree, d_tree, sk_d, tid_context)
                self._calc_tree_structure_diff(s_tree, d_tree, tid_context)
        return False, None
    
    def _handle_trees_diffs(self, src, dest, skip_dict, context, key_in_par):
        trees_skip_d = {'^ot:treeElementOrder': self.no_op_t, 
                        'treeById': (self._handle_tree_diffs, None)}
        sub_context = context.child(key_in_par)
        trees_id_dict = {}
        self._trees_by_id_dict = trees_id_dict
        for trees_id, s_trees in src.items():
            tsid_context = sub_context.child(trees_id)
            d_trees = dest.get(trees_id)
            if d_trees is not None:
                tree_id_dict = {}
                trees_id_dict[trees_id] = tree_id_dict
                self._process_ordering_pair('^ot:treeElementOrder',
                                            'treeById',
                                            '_tree_order',
                                            tree_id_dict,
                                            s_trees,
                                            d_trees)
                self._calculate_generic_diffs(s_trees, d_trees, trees_skip_d, tsid_context)
        return False, None

    def _calc_tree_structure_diff(self, s_tree, d_tree, context):
        edge_skip = {'@source': self.no_op_t, '@target': self.no_op_t}
        s_edges_bsid = s_tree['edgeBySourceId']
        d_edges_bsid = d_tree['edgeBySourceId']
        s_node_bid = s_tree['nodeById']
        d_node_bid = d_tree['nodeById']
        s_root = s_tree['^ot:rootNodeId']
        d_root = d_tree['^ot:rootNodeId']
        roots_equal = (s_root == d_root)
        nodes_equal = (s_node_bid == d_node_bid)
        edges_equal = (s_edges_bsid == d_edges_bsid)
        if roots_equal and nodes_equal and edges_equal:
            return

        s_node_id_set = set(s_node_bid.keys())
        d_node_id_set = set(d_node_bid.keys())
        
        node_number_except = False
        s_extra_node_id = s_node_id_set - d_node_id_set
        if s_extra_node_id:
            if (len(s_extra_node_id) > 1) or (s_extra_node_id.pop() != s_root):
                node_number_except = True
        d_extra_node_id = d_node_id_set - s_node_id_set
        if d_extra_node_id:
            if (len(d_extra_node_id) > 1) or (d_extra_node_id.pop() == d_root):
                node_number_except = True
        #TODO: do we need more flexible diffs. In normal curation, there can 
        #   only be one node getting added (the root)
        if node_number_except:
            raise ValueError('At most one node and one edge can be added to a tree')
        
        self.activate_tree_diffs()
        try:
            deleted_node = None
            added_node = None
            deleted_edge = None
            added_edge = None
            if not nodes_equal:
                sub_context = context.child('nodeById')
                for nid, s_node in s_node_bid.items():
                    d_node = d_node_bid.get(nid)
                    if d_node is not None:
                        if d_node != s_node:
                            n_context = sub_context.child(nid)
                            self._calculate_generic_diffs(s_node, d_node, None, n_context)
                    else:
                        assert(deleted_node is None)
                        deleted_node = (nid, s_node)
                    #    n_context = sub_context.child(nid)
                    #    self.add_deletion(n_context)
                if d_root not in s_node_id_set:
                    added_node = (d_root, d_node_bid.get(d_root))
                #    d_node = d_node_bid.get(d_root)
                #    n_context = sub_context.child(d_root)
                #    self.add_addition(d_node, n_context)
            if not edges_equal:
                sub_context = context.child('edgeBySourceId')
                s_edges = edge_by_source_to_edge_dict(s_edges_bsid)
                d_edges = edge_by_source_to_edge_dict(d_edges_bsid)
                lde = len(d_edges)
                lse = len(s_edges)

                for eid, s_edge in s_edges.items():
                    d_edge = d_edges.get(eid)
                    if d_edge is None:
                        assert(deleted_edge is None)
                        deleted_edge = (eid, s_edge)
                    else:
                        if d_edge != s_edge:
                            e_context = sub_context.child(eid)
                            if (len(s_edge) > 3) or (len(d_edge) > 3) \
                                or ('@length' not in s_edge) \
                                or ('@length' not in d_edge):
                                self._calculate_generic_diffs(s_node, d_node, edge_skip, e_context)
                            elif s_edge['@length'] != d_edge['@length']:
                                self.add_modification(d_edge['@length'], e_context)
                            s_s, s_t = s_edge['@source'], s_edge['@target']
                            d_s, d_t = d_edge['@source'], d_edge['@target']
                            if ((s_s == d_s) and (s_t == d_t)) or ((s_s == d_t) and (s_t == d_s)):
                                pass
                            else:
                                raise_except = True
                                if deleted_node and deleted_node[0] == s_s:
                                    opp_nd = None
                                    if s_t == d_s:
                                        opp_nd = d_t
                                    elif s_t == d_t:
                                        opp_nd = d_s
                                    if opp_nd:
                                        s_root_edges = s_edges_bsid[s_s]
                                        for se in s_root_edges.values():
                                            if se['@target'] == opp_nd:
                                                raise_except = False
                                                break
                                elif added_node and added_node[0] == d_s:
                                    opp_nd = None
                                    if d_t == s_s:
                                        opp_nd = s_t
                                    elif d_t == s_t:
                                        opp_nd = s_s
                                    if opp_nd:
                                        d_root_edges = d_edges_bsid[d_s]
                                        for de in d_root_edges.values():
                                            if de['@target'] == opp_nd:
                                                raise_except = False
                                                break
                                if raise_except:
                                    msgf = 'Tree structure altered "{ds}"->"{dt}" not found in "{ss}"->"{st}"'
                                    msg = msgf.format(ds=d_s, dt=d_t, ss=s_s, st=s_t)
                                    raise ValueError(msg)

                if lde - lse > 0:
                    s_eid_set = set(s_edges.keys())
                    d_eid_set = set(d_edges.keys())
                    extra_eid_set = d_eid_set - s_eid_set
                    if lde - lse > 1:
                        msgf = 'More than one extra edge in the destination tree: "{}"'
                        msg = msgf.format('", "'.join(i for i in extra_eid_set))
                        raise ValueError(msg)
                    aeid = extra_eid_set.pop()
                    ae = d_edges[aeid]
                    added_edge = (aeid, ae)
                self.add_rerooting((deleted_node, deleted_edge), (added_node, added_edge), context)
        finally:
            self.activate_nontree_diffs()

    def add_rerooting(self, del_nd_edge, add_nd_edge, context):
        self._rerootings.append((del_nd_edge, add_nd_edge, context))

    def activate_tree_diffs(self):
        self.curr_diff_dict = self._tree_diff
        self.curr_unapplied_dict = self._unapplied_tree_edits
        self.curr_redundant_dict = self._redundant_tree_edits


    def activate_nontree_diffs(self):
        self.curr_diff_dict = self._nontree_diff
        self.curr_unapplied_dict = self._unapplied_nontree_edits
        self.curr_redundant_dict = self._redundant_nontree_edits

    def add_addition(self, v, context):
        self.curr_diff_dict['additions'].append((v, context))

    def add_deletion(self, context):
        self.curr_diff_dict['deletions'].append(context)

    def add_modification(self, v, context):
        self.curr_diff_dict['modifications'].append((v, context))

    def _calculate_generic_diffs(self, src, dest, skip_dict, context):
        sk = set(src.keys())
        dk = set(dest.keys())
        if skip_dict is None:
            skip_dict = {}
        sk.update(dk)
        for k in sk:
            self._calc_diff_for_key(k, src, dest, skip_dict, context)
        return self

    def _calc_diff_for_key(self, k, src, dest, skip_dict, context):
        sub_context = None
        if k in src:
            do_generic_calc = True
            v = src[k]
            skip_tuple = skip_dict.get(k)
            sub_skip_dict = None
            sub_context = None
            if skip_tuple is not None:
                func, sub_skip_dict = skip_tuple
                do_generic_calc, sub_context = func(v, dest.get(k), sub_skip_dict, context=context, key_in_par=k)
            if do_generic_calc:
                if k in dest:
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
        elif k in dest:
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
