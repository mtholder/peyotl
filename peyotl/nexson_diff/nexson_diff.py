 #!/usr/bin/env python
'''Functions for diffing and patching nexson blobs

'''
from peyotl.nexson_syntax import detect_nexson_version, \
                                 read_as_json, \
                                 write_as_json, \
                                 _is_by_id_hbf, \
                                 edge_by_source_to_edge_dict
from peyotl.nexson_diff.nexson_diff_address import NexsonDiffAddress
from peyotl.utility import get_logger
import itertools
import json

_LOG = get_logger(__name__)

def _get_blob(src):
    '''Returns the nexson blob for diffing from filepath or dict.
    Verifies that the nexson version is correct (or raises a ValueError)
    '''
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

OT_DIFF_TYPE_LIST = ('additions', 'deletions', 'modifications')

def new_diff_summary(in_tree=False):
    d = {}
    add_diff_fields(d, in_tree=in_tree)
    return d

def add_diff_fields(d, in_tree=False):
    for k in OT_DIFF_TYPE_LIST:
        if k not in d:
            d[k] = []
    if in_tree:
        if 'rerootings' not in d:
            d['rerootings'] = []
    else:
        d['key-ordering'] = {}
    return d

def add_nested_diff_fields(d):
    if 'tree' not in d:
        d['tree'] = new_diff_summary(in_tree=True)
    else:
        add_diff_fields(d['tree'], in_tree=True)
    add_diff_fields(d, False)

def new_nested_diff_summary():
    outer = {}
    add_nested_diff_fields(outer)
    return outer, outer['tree']

def _create_empty_ordered_edit(by_id_property, order_property, address):
    return {'by_id_property': by_id_property,
            'deleted_id_set': set(),
            'order_property': order_property,
            'address': address,
            'added_id_map': {},
    }

def _add_in_order(dest, new_items, full_list, set_contained):
    prev = None
    nsi = set(new_items)
    for i in full_list:
        if i in set_contained:
            prev = i
        elif i in nsi:
            if prev is None:
                dest.push(0, i)
            else:
                prev_ind = dest.index(prev)
                dest.insert(prev_ind + 1, i)
                prev = i

def _register_ordered_edit_not_made(container,
                                    edit_core,
                                    by_id_property,
                                    order_property,
                                    address,
                                    dest_order,
                                    known_order_key,
                                    group_id):
    full_edit = _create_empty_ordered_edit(by_id_property, order_property, address)
    rdis = edit_core.get('deleted_id_set')
    if rdis:
        full_edit['deleted_id_set'] = rdis
    raim = edit_core.get('added_id_map')
    if raim:
        full_edit['added_id_map'] = raim
    d_o = edit_core.get('dest_order')
    if d_o is not None:
        full_edit['dest_order'] = d_o
    uoe = container.setdefault('key-ordering', {})

    if full_edit is None:
        uoe.setdefault(known_order_key, full_edit)
    else:
        utbgi = uoe.setdefault(known_order_key, {})
        utbgi[group_id] = full_edit

def _apply_ordered_edit_to_group(nexson_diff,
                                 base_container,
                                 the_edit,
                                 known_order_key,
                                 group_id):
    #_LOG.debug('the_edit = ' + str(the_edit))
    bip = the_edit['by_id_property']
    op = the_edit['order_property']
    dest_order = the_edit['dest_order']
    orig_order_of_retained = the_edit['orig_order_of_retained']
    red_ed = {}
    unap_ed = {}

    by_id_coll = base_container[bip]
    order_coll = base_container[op]
    base_set = set(order_coll)
    added = set()
    added_order = []
    # do the deletion...
    #_LOG.debug('deleted_id_set = {}'.format(the_edit.get('deleted_id_set', [])))
    for k in the_edit.get('deleted_id_set', []):
        if k not in order_coll:
            #_LOG.debug('already deleted ' + k)
            red_ed.setdefault('deleted_id_set', set()).add(k)
        else:
            del by_id_coll[k]
            order_coll.remove(k)
    for k, v in the_edit.get('added_id_map', {}).items():
        if k in order_coll:
            unap_ed.setdefault('added_id_map', {})[k] = v
        else:
            by_id_coll[k] = v
            added.add(k)
            added_order.append(k)

    #TODO: better ordering when both have changed?
    orig_set = set(orig_order_of_retained)
    edit_set = set(dest_order)
    in_all_three = edit_set.intersection(base_set).intersection(orig_set)
    orig_order_3 = [i for i in orig_order_of_retained if i in in_all_three]
    base_order_3 = [i for i in order_coll if i in in_all_three]
    edit_order_3 = [i for i in dest_order if i in in_all_three]
    if (orig_order_3 == base_order_3) and(orig_order_3 != edit_order_3):
        final_order = edit_order_3
    else:
        final_order = base_order_3
    added_in_base = (set(order_coll)  - added) - in_all_three
    if added_in_base:
        added_in_base_order = [i for i in order_coll if i in added_in_base]
        _add_in_order(final_order, added_in_base_order, order_coll, in_all_three)
    if added:
        _add_in_order(final_order, added_order, dest_order, in_all_three)
    base_container[op] = final_order

    if red_ed:
        _register_ordered_edit_not_made(nexson_diff._redundant_edits,
                                        red_ed,
                                        bip,
                                        op,
                                        the_edit['address'],
                                        the_edit['dest_order'],
                                        known_order_key,
                                        group_id)
    if unap_ed:
        _register_ordered_edit_not_made(nexson_diff._unapplied_edits,
                                        unap_ed,
                                        bip,
                                        op,
                                        the_edit['address'],
                                        the_edit['dest_order'],
                                        known_order_key,
                                        group_id)

_KNOWN_ORDERED_KEYS = {'treesById':('treesById', True),
                       'trees': (None, False),
                       'otus': (None, False)}

def _ordering_patch_modified_blob(nexson_diff, base_blob, ordering_dict):
    #_LOG.debug('ordering dict = ' + str(ordering_dict))
    for k in ordering_dict.keys():
        assert k in _KNOWN_ORDERED_KEYS
    base_nexml = base_blob['nexml']
    for k, t in _KNOWN_ORDERED_KEYS.items():
        bk, nested = t
        g = ordering_dict.get(k)
        if g is not None:
            if bk is None:
                bg = base_nexml
            else:
                bg = base_nexml.get(bk)
            if bg is None:
                nexson_diff._unapplied_edits.setdefault('key-ordering', {})[k] = g
            else:
                if nested:
                    for group_id, the_edit in g.items():
                        tg = bg.get(group_id)
                        if tg is None:
                            #_LOG.debug('unapplied tree edit for lack of group_id ' + group_id)
                            uoe = nexson_diff._unapplied_edits.setdefault('key-ordering', {})
                            utbgi = uoe.setdefault(k, {})
                            utbgi[group_id] = the_edit
                            continue
                        _apply_ordered_edit_to_group(nexson_diff,
                                                     tg,
                                                     the_edit,
                                                     k,
                                                     group_id)
                else:
                    _apply_ordered_edit_to_group(nexson_diff,
                                                 bg,
                                                 g,
                                                 k,
                                                 None)

def _dict_patch_modified_blob(nexson_diff, base_blob, diff_dict):
    dels = diff_dict['deletions']
    adds = diff_dict['additions']
    mods = diff_dict['modifications']
    for v, c in dels:
        c.try_apply_del_to_mod_blob(nexson_diff, base_blob, v)
    adds_to_mods = []
    really_adds = set()
    for t in adds:
        v, c = t
        added, is_mod = c.try_apply_add_to_mod_blob(nexson_diff, base_blob, v, False)
        if not added:
            if is_mod:
                #_LOG.debug('add t = {}'.format(t))
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

def _tree_dict_patch_modified_blob(nexson_diff, base_blob, diff_dict):
    rerootings = diff_dict['rerootings']
    for reroot_info, c in rerootings:
        c.try_apply_rerooting_to_mod_blob(nexson_diff, base_blob, reroot_info)
    _dict_patch_modified_blob(nexson_diff, base_blob, diff_dict)

def _ordering_to_ot_diff(od):
    r = {}
    for k, v in od.items():
        if k == 'address':
            x = v.as_ot_target()
            r.update(x)
        else:
            if isinstance(v, set):
                r[k] = list(v)
            else:
                r[k] = v
    return r

def _dict_of_ordering_to_ot_diff(od):
    #_LOG.debug(od.keys())
    nested_key = 'treesById'
    r = {}
    for k, v in od.items():
        if k != nested_key:
            r[k] = _ordering_to_ot_diff(v)
        else:
            n = {}
            r[nested_key] = n
            for k2, v2 in v.items():
                n[k2] = _ordering_to_ot_diff(v2)
    return r

def _to_ot_diff_dict(native_diff):
    r = {}
    for dt in ('additions', 'modifications', 'deletions'):
        kvc_list = native_diff.get(dt, [])
        x = []
        for v, c in kvc_list:
            if isinstance(v, set):
                vl = list(v)
                vl.sort()
                y = {'value': vl}
            else:
                y = {'value': v}
            if c is not None:
                y.update(c.as_ot_target())
            x.append(y)
        if kvc_list:
            r[dt] = x
    x = native_diff.get('tree')
    if x:
        todd = _to_ot_diff_dict(x)
        if todd:
            r['tree'] = todd
    if 'key-ordering' in native_diff:
        nk = native_diff['key-ordering']
        if nk:
            r['key-ordering'] = _dict_of_ordering_to_ot_diff(nk)
    return r

def _process_order_list_and_dict(order_key, by_id_key, src, dest):
    src_order = src.get(order_key, [])
    dest_order = dest.get(order_key, [])
    if src_order == dest_order:
        return {}
    src_set = set(src_order)
    dest_set = set(dest_order)
    dest_otus = dest.get(by_id_key, {})
    ret_set = set()
    add_id_map = {}
    del_id_set = set()
    for o in dest_set:
        if o in src_set:
            ret_set.add(o)
        else:
            add_id_map[o] = dest_otus[o]
    orig_id_order = []
    for o in src_order:
        if o in ret_set:
            orig_id_order.append(o)

    for o in src_set:
        if not o in dest_order:
            del_id_set.add(o)
    return {'dest_order': dest_order,
            'orig_order_of_retained': orig_id_order,
            'added_id_map': add_id_map,
            'deleted_id_set': del_id_set}

_SET_LIKE_PROPERTIES = frozenset([
    '^ot:curatorName',
    '^ot:tag',
    '^ot:candidateTreeForSynthesis',
    '^skos:altLabel',
    '^ot:dataDeposit'])
_BY_ID_LIST_PROPERTIES = frozenset(['agent', 'annotation'])
_NO_MOD_PROPERTIES = frozenset(['message'])



class NexsonDiff(object):
    def __init__(self, anc=None, des=None, patch=None):
        if patch is None:
            if anc is None or des is None:
                raise ValueError('if "patch" is not supplied, both "anc" and "des" must be supplied.')
            self.anc_blob = _get_blob(anc)
            self.des_blob = _get_blob(des)
            self.no_op_t = (self._no_op_handling, None)
            self._calculate_diff()
        else:
            self.diff_dict = patch
            add_nested_diff_fields(patch)

    def patch_modified_file(self,
                            filepath_to_patch=None,
                            input_nexson=None,
                            output_filepath=None):
        '''Take a NexSON (via filepath_to_patch or input_nexson dict) and applies
        the diffs (stored in the `self` object) to that NexSON and then
        writes the output to a file. output_filepath
        if output_filepath is `None` then filepath_to_patch will be used as the
        output_filepath.
        NexsonDiff.patch_modified_blob does the patch
        '''
        if input_nexson is None:
            assert isinstance(filepath_to_patch, str) or isinstance(filepath_to_patch, unicode)
            input_nexson = _get_blob(filepath_to_patch)
        else:
            v = detect_nexson_version(input_nexson)
            if not _is_by_id_hbf(v):
                raise ValueError('NexsonDiff objects can only operate on NexSON version 1.2. Found version = "{}"'.format(v))
        self.patch_modified_blob(input_nexson)
        if output_filepath is None:
            output_filepath = filepath_to_patch
        write_as_json(input_nexson, output_filepath)

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
        n, t = new_nested_diff_summary()
        self._unapplied_nontree_edits = n
        self._unapplied_tree_edits = t

        n, t = new_nested_diff_summary()
        self._redundant_nontree_edits = n
        self._redundant_tree_edits = t

        self._redundant_edits = self._redundant_nontree_edits
        self._unapplied_edits = self._unapplied_nontree_edits

    def _clear_diff_related_data(self):
        self._nontree_diff, self._tree_diff = new_nested_diff_summary()
        self.diff_dict = self._nontree_diff
        self.activate_nontree_diffs()
        # the following is not mutable, so it could go in __init__
        otus_diff = (self._handle_otus_diffs, None)
        trees_diff = (self._handle_trees_diffs, None)
        nexml_diff = (self._handle_nexml_diffs, {'otusById': otus_diff,
                                                 '^ot:otusElementOrder': self.no_op_t,
                                                 '^ot:treesElementOrder': self.no_op_t,
                                                 'treesById': trees_diff})
        self.top_skip_dict = {'nexml': nexml_diff}


    def patch_modified_blob(self, base_blob):
        '''Applies the diff stored in `self` to the NexSON dict `base_blob`
        self._redundant_edits and self._unapplied_edits will be reset so that
        they reflect the edits that were not applied (either because the edits
        were already found in `base_blob` [_redundnant_edits or because the
        appropriate operations could not be performed on the base_blob dict)
        '''
        #_LOG.debug('base_blob[nexml]["^ot:agents"] = {}'.format(base_blob["nexml"]["^ot:agents"]))
        self._clear_patch_related_data()
        d = self.diff_dict
        _dict_patch_modified_blob(self, base_blob, d)
        self._redundant_edits = self._redundant_tree_edits
        self._unapplied_edits = self._unapplied_tree_edits
        _tree_dict_patch_modified_blob(self, base_blob, d['tree'])
        _ordering_patch_modified_blob(self, base_blob, d['key-ordering'])

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
                               dest,
                               context):
        d = {} if (dest is None) else dest
        s = {} if (src is None) else src
        r = _process_order_list_and_dict(order_key, by_id_key, s, d)
        if r:
            r['address'] = context
            r['order_property'] = order_key
            r['by_id_property'] = by_id_key
            #_LOG.debug('storage_key = ' + storage_key)
            storage_container[storage_key] = r

    def _no_op_handling(self, src, dest, skip_dict, context, key_in_par):
        return False, None

    def _handle_nexml_diffs(self, src, dest, skip_dict, context, key_in_par):
        container = self._nontree_diff['key-ordering']
        sc = context.child('nexml')
        self._process_ordering_pair('^ot:otusElementOrder',
                                    'otusById',
                                    'otus',
                                    container,
                                    src,
                                    dest,
                                    sc)
        self._process_ordering_pair('^ot:treesElementOrder',
                                    'treesById',
                                    'trees',
                                    container,
                                    src,
                                    dest,
                                    sc)
        return True, sc

    def _normal_handling(self, src, dest, skip_dict, context, key_in_par):
        return True, None

    def _handle_otus_diffs(self, src, dest, skip_dict, context, key_in_par):
        sub_context = context.child(key_in_par)
        for otus_id, s_otus in src.items():
            otusid_context = sub_context.child(otus_id)
            d_otus = dest.get(otus_id)
            if d_otus is not None:
                assert d_otus.keys() == ['otuById']
                assert s_otus.keys() == ['otuById']
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
        tbgi = self._nontree_diff['key-ordering'].setdefault('treesById', {})
        #_LOG.debug(self._nontree_diff['key-ordering'].keys())
        for trees_id, s_trees in src.items():
            tsid_context = sub_context.child(trees_id)
            d_trees = dest.get(trees_id)
            if d_trees is not None:
                self._process_ordering_pair('^ot:treeElementOrder',
                                            'treeById',
                                            trees_id,
                                            tbgi,
                                            s_trees,
                                            d_trees,
                                            tsid_context)
                self._calculate_generic_diffs(s_trees, d_trees, trees_skip_d, tsid_context)
            else:
                self.add_deletion(s_trees, context=tsid_context)
        return False, None

    def _calc_tree_diff_no_rooting_change(self, s_tree, d_tree, context):
        edge_skip = {'@source': self.no_op_t, '@target': self.no_op_t}
        s_edges_bsid = s_tree['edgeBySourceId']
        d_edges_bsid = d_tree['edgeBySourceId']
        s_node_bid = s_tree['nodeById']
        d_node_bid = d_tree['nodeById']
        s_node_id_set = set(s_node_bid.keys())
        d_node_id_set = set(d_node_bid.keys())
        nodes_equal = (s_node_bid == d_node_bid)
        edges_equal = (s_edges_bsid == d_edges_bsid)
        if s_node_id_set != d_node_id_set:
            raise ValueError('If the rooting does not change, the nodes IDs should be identical.')
        self.activate_tree_diffs()
        t_context = None
        try:
            if not nodes_equal:
                if t_context is None:
                    t_context = context.create_tree_context()
                sub_context = t_context.child('nodeById')
                for nid, s_node in s_node_bid.items():
                    d_node = d_node_bid.get(nid)
                    assert d_node is not None
                    if d_node != s_node:
                        n_context = sub_context.child(nid)
                        self._calculate_generic_diffs(s_node, d_node, None, n_context)
            if not edges_equal:
                if t_context is None:
                    t_context = context.create_tree_context()
                sub_context = t_context.edge_child()
                s_edges = edge_by_source_to_edge_dict(s_edges_bsid)
                d_edges = edge_by_source_to_edge_dict(d_edges_bsid)
                lde = len(d_edges)
                lse = len(s_edges)
                _LOG.debug('s_edges = {}'.format(s_edges))
                _LOG.debug('d_edges = {}'.format(d_edges))
                for eid, s_edge in s_edges.items():
                    _LOG.debug('eid = {}'.format(eid))
                    d_edge = d_edges.get(eid)
                    assert d_edge is not None
                    if d_edge != s_edge:
                        e_context = sub_context.child(eid)
                        if (len(s_edge) > 3) or (len(d_edge) > 3) \
                            or ('@length' not in s_edge) \
                            or ('@length' not in d_edge):
                            self._calculate_generic_diffs(s_node, d_node, edge_skip, e_context)
                        elif s_edge['@length'] != d_edge['@length']:
                            self.add_modification(d_edge['@length'], e_context)
        finally:
            self.activate_nontree_diffs()

    def _calc_tree_structure_diff(self, s_tree, d_tree, context):
        edge_skip = {'@source': self.no_op_t, '@target': self.no_op_t}
        s_edges_bsid = s_tree['edgeBySourceId']
        d_edges_bsid = d_tree['edgeBySourceId']
        s_node_bid = s_tree['nodeById']
        d_node_bid = d_tree['nodeById']
        s_root_id = s_tree['^ot:rootNodeId']
        d_root_id = d_tree['^ot:rootNodeId']
        roots_equal = (s_root_id == d_root_id)
        nodes_equal = (s_node_bid == d_node_bid)
        edges_equal = (s_edges_bsid == d_edges_bsid)
        if roots_equal and nodes_equal and edges_equal:
            return
        if roots_equal:
            self._calc_tree_diff_no_rooting_change(s_tree, d_tree, context)
            return
        s_node_id_set = set(s_node_bid.keys())
        d_node_id_set = set(d_node_bid.keys())

        node_number_except = False
        s_extra_node_id = s_node_id_set - d_node_id_set
        if s_extra_node_id:
            _LOG.debug("s_extra_node_id = {}".format(s_extra_node_id))
            if (len(s_extra_node_id) > 1) or (s_extra_node_id.pop() != s_root_id):
                node_number_except = True
        d_extra_node_id = d_node_id_set - s_node_id_set
        if d_extra_node_id:
            _LOG.debug("d_extra_node_id = {}".format(d_extra_node_id))
            if (len(d_extra_node_id) > 1) or (d_extra_node_id.pop() != d_root_id):
                node_number_except = True
        #TODO: do we need more flexible diffs. In normal curation, there can
        #   only be one node getting added (the root)
        if node_number_except:
            raise ValueError('At most one node and one edge can be added to a tree')
        self.activate_tree_diffs()
        t_context = None
        try:
            reroot_info = {'add_edge': None,
                           'add_edge_id': None,
                           'add_node': None,
                           'add_node_id': None,
                           'add_node_children': None,
                           'del_edge': None,
                           'del_edge_id': None,
                           'del_node': None,
                           'del_node_id': None,
                           'new_root_id': None,
                           'add_edge_sib_edge': None,
                           'add_edge_sib_edge_id': None,
            }
            if not nodes_equal:
                if t_context is None:
                    t_context = context.create_tree_context()
                sub_context = t_context.child('nodeById')
                for nid, s_node in s_node_bid.items():
                    d_node = d_node_bid.get(nid)
                    if d_node is not None:
                        if d_node != s_node:
                            n_context = sub_context.child(nid)
                            self._calculate_generic_diffs(s_node, d_node, None, n_context)
                    else:
                        assert reroot_info['del_node'] is None
                        reroot_info['del_node'] = s_node
                        reroot_info['del_node_id'] = nid
                    #    n_context = sub_context.child(nid)
                    #    self.add_deletion(n_context)
                if d_root_id not in s_node_id_set:
                    reroot_info['add_node_id'] = d_root_id
                    reroot_info['add_node'] = d_node_bid.get(d_root_id)
            if not edges_equal:
                if t_context is None:
                    t_context = context.create_tree_context()
                sub_context = t_context.edge_child()
                s_edges = edge_by_source_to_edge_dict(s_edges_bsid)
                d_edges = edge_by_source_to_edge_dict(d_edges_bsid)
                lde = len(d_edges)
                lse = len(s_edges)

                for eid, s_edge in s_edges.items():
                    d_edge = d_edges.get(eid)
                    if d_edge is None:
                        assert reroot_info['del_edge'] is None
                        reroot_info['del_edge'] = s_edge
                        reroot_info['del_edge_id'] = eid
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
                                if reroot_info['del_node_id'] == s_s:
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
                                elif reroot_info['add_node_id'] == d_s:
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
                s_eid_set = set(s_edges.keys())
                d_eid_set = set(d_edges.keys())
                extra_eid_set = d_eid_set - s_eid_set
                if extra_eid_set:
                    if len(extra_eid_set) > 1:
                        msgf = 'More than one extra edge in the destination tree: "{}"'
                        msg = msgf.format('", "'.join(i for i in extra_eid_set))
                        raise ValueError(msg)
                    aeid = extra_eid_set.pop()
                    ae = d_edges[aeid]
                    reroot_info['add_edge_id'], reroot_info['add_edge'] = aeid, ae
                    aes = ae['@source']
                    assert aes == d_root_id
                    root_edges = d_edges_bsid[aes]
                    assert len(root_edges) == 2
                    sib_edge = None
                    sib_edge_id = None
                    for k, v in root_edges.items():
                        if k != aeid:
                            sib_edge = v
                            sib_edge_id = k
                    assert sib_edge is not None
                    reroot_info['add_edge_sib_edge'] = v
                    reroot_info['add_edge_sib_edge_id'] = k
                if d_root_id != s_root_id:
                    if t_context is None:
                        t_context = context.create_tree_context()
                    reroot_info['new_root_id'] = d_root_id
                    self.add_rerooting(reroot_info, t_context)
                else:
                    assert reroot_info['del_node'] is None
                    assert reroot_info['del_edge'] is None
                    assert reroot_info['add_node_id'] is None
                    assert reroot_info['add_edge_id'] is None
        finally:
            self.activate_nontree_diffs()

    def add_rerooting(self, reroot_info, context):
        self.curr_diff_dict['rerootings'].append((reroot_info, context))

    def activate_tree_diffs(self):
        self.curr_diff_dict = self._tree_diff

    def activate_nontree_diffs(self):
        self.curr_diff_dict = self._nontree_diff

    def add_addition(self, v, context):
        self.curr_diff_dict['additions'].append((v, context))

    def add_deletion(self, v, context):
        self.curr_diff_dict['deletions'].append((v, context))

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
                            if isinstance(v, list) or isinstance(dv, list):
                                if not isinstance(v, list):
                                    v = [v]
                                if not isinstance(dv, list):
                                    dv = [dv]
                                if k in _SET_LIKE_PROPERTIES:
                                    dvs = set(dv)
                                    svs = set(v)
                                    dels = svs - dvs
                                    adds = dvs - svs
                                    if adds or dels:
                                        sub_context = context.set_like_list_child(k)
                                elif k in _BY_ID_LIST_PROPERTIES:
                                    dd = {i['@id']:i for i in dv}
                                    sd = {i['@id']:i for i in v}
                                    sds = set(sd.keys())
                                    dds = set(dd.keys())
                                    adds = dds - sds
                                    dels = sds - dds
                                    if adds:
                                        adds = tuple([dd[i] for i in adds])
                                    #_LOG.debug('_BY_ID_LIST_PROPERTIES dels = {}'.format(str(dels)))
                                    #_LOG.debug('_BY_ID_LIST_PROPERTIES adds = {}'.format(str(adds)))
                                    sub_context = context.by_id_list_child(k)
                                    inters = sds.intersection(dds)
                                    for ki in inters:
                                        dsv = dd[ki]
                                        ssv = sd[ki]
                                        if dsv != ssv:
                                            #_LOG.debug('{} != {}'.format(dsv, ssv))
                                            sub_sub_context = sub_context.child(ki)
                                            self._calculate_generic_diffs(ssv, dsv, skip_dict=sub_skip_dict, context=sub_sub_context)
                                else:
                                    # treat like _NO_MOD_PROPERTIES
                                    # ugh not efficient...
                                    dels, adds = detect_no_mod_list_dels_adds(v, dv)
                                    if adds or dels:
                                        sub_context = context.no_mod_list_child(k)
                                if dels:
                                    self.add_deletion(dels, context=sub_context)
                                if adds:
                                    self.add_addition(adds, context=sub_context)
                                
                            else:
                                if k in _SET_LIKE_PROPERTIES:
                                    sub_context = context.set_like_list_child(k)
                                else:
                                    sub_context = context.child(k)
                                #_LOG.debug('mod key "{}" from "{}" to "{}"'.format(k, v, dv))
                                self.add_modification(dv, context=sub_context)
                else:
                    if sub_context is None:
                        sub_context = context.child(k)
                    #_LOG.debug('del key "{}"'.format(k))
                    self.add_deletion(v, context=sub_context)
        elif k in dest:
            do_generic_calc = True
            skip_tuple = skip_dict.get(k)
            sub_skip_dict = None
            if skip_tuple is not None:
                func, sub_skip_dict = skip_tuple
                do_generic_calc, sub_context = func(v, dest.get(k), sub_skip_dict, context=context, key_in_par=k)
            if do_generic_calc:
                if sub_context is None:
                    if k in _SET_LIKE_PROPERTIES:
                        sub_context = context.set_like_list_child(k)
                    else:
                        sub_context = context.child(k)
                #_LOG.debug('add key "{}" from "{}"'.format(k, dest[k]))
                self.add_addition(dest[k], context=sub_context)


def detect_no_mod_list_dels_adds(src, dest):
    '''Takes an ancestor (src),  descendant (dest) pair of lists
    and returns a pair of lists:
        dels = the objects in src for which there is not object in dest that compares equal to
        adds = the objects in dest for which there is not object in src that compares equal to
    '''
    dfound_set = set()
    sunfound_set = set()
    dels, adds = [], []
    for sn, el in enumerate(src):
        found = False
        for dn, d_el in enumerate(dest):
            if el == d_el:
                dfound_set.add(dn)
                found = True
                break
        if not found:
            dels.append(el)
            sunfound_set.add(sn)
    for dn, d_el in enumerate(dest):
        if dn not in dfound_set:
            found = False
            for sn, el in enumerate(src):
                if sn not in sunfound_set:
                    if el == d_el:
                        found = True
            if not found:
                adds.append(d_el)
    return dels, adds