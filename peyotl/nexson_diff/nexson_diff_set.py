#!/usr/bin/env python
'''Functions for diffing and patching nexson blobs

'''
from peyotl.nexson_syntax import detect_nexson_version, \
                                 read_as_json, \
                                 write_as_json, \
                                 _is_by_id_hbf, \
                                 edge_by_source_to_edge_dict
from peyotl.nexson_diff.nexson_diff_address import NexsonDiffAddress
from peyotl.nexson_diff.nexson_diff import RerootingDiff, \
                                           DeletionDiff, \
                                           NoModDelDiff, \
                                           AdditionDiff, \
                                           ByIdAddDiff, \
                                           NoModAddDiff, \
                                           ModificationDiff, \
                                           SetAddDiff, \
                                           SetDelDiff, \
                                           ByIdDelDiff, \
                                           ElementOrderDiff
                                           
from peyotl.nexson_diff.patch_log import PatchLog
from peyotl.nexson_diff.helper import extract_by_diff_type, \
                                      new_diff_summary, \
                                      to_ot_diff_dict, \
                                      dict_patch_modified_blob, \
                                      detect_no_mod_list_dels_adds

from peyotl.utility import get_logger
import itertools
import copy
import json

_SET_LIKE_PROPERTIES = frozenset([
    '^ot:curatorName',
    '^ot:tag',
    '^ot:candidateTreeForSynthesis',
    '^skos:altLabel',
    '^ot:dataDeposit'])
_BY_ID_LIST_PROPERTIES = frozenset(['agent', 'annotation'])
_NO_MOD_PROPERTIES = frozenset(['message'])

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
        raise ValueError('NexsonDiffSet objects can only operate on NexSON version 1.2. Found version = "{}"'.format(v))
    return b

class NexsonDiffSet(object):
    def __init__(self, anc=None, des=None, patch=None):
        self.no_op_t = (self._no_op_handling, None)
        otus_diff = (self._handle_otus_diffs, None)
        trees_diff = (self._handle_trees_diffs, None)
        nexml_diff = (self._handle_nexml_diffs, {'otusById': otus_diff,
                                                 '^ot:otusElementOrder': self.no_op_t,
                                                 '^ot:treesElementOrder': self.no_op_t,
                                                 'treesById': trees_diff})
        self.top_skip_dict = {'nexml': nexml_diff}

        if patch is None:
            if anc is None or des is None:
                raise ValueError('if "patch" is not supplied, both "anc" and "des" must be supplied.')
            self.anc_blob = _get_blob(anc)
            self.des_blob = _get_blob(des)
            self._calculate_diffs()
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
        NexsonDiffSet.patch_modified_blob does the patch
        '''
        if input_nexson is None:
            assert isinstance(filepath_to_patch, str) or isinstance(filepath_to_patch, unicode)
            input_nexson = _get_blob(filepath_to_patch)
        else:
            v = detect_nexson_version(input_nexson)
            if not _is_by_id_hbf(v):
                raise ValueError('NexsonDiffSet objects can only operate on NexSON version 1.2. Found version = "{}"'.format(v))
        patch_log = self.patch_modified_blob(input_nexson)
        if output_filepath is None:
            output_filepath = filepath_to_patch
        write_as_json(input_nexson, output_filepath)
        return patch_log

    def as_ot_diff_dict(self):
        return to_ot_diff_dict(self.diff_dict)

    def has_differences(self):
        d = self.diff_dict
        for k in d.keys():
            if d.get(k):
                return True
        return False

    def _clear_diff_related_data(self):
        self.diff_dict = new_diff_summary()

    def patch_modified_blob(self, base_blob):
        '''Applies the diff stored in `self` to the NexSON dict `base_blob`
        self._redundant_edits and self._unapplied_edits will be reset so that
        they reflect the edits that were not applied (either because the edits
        were already found in `base_blob` [_redundnant_edits or because the
        appropriate operations could not be performed on the base_blob dict)
        '''
        #_LOG.debug('base_blob[nexml]["^ot:agents"] = {}'.format(base_blob["nexml"]["^ot:agents"]))
        patch_log = PatchLog()
        d = self.diff_dict
        dict_patch_modified_blob(base_blob, d, patch_log)
        return patch_log

    def _calculate_diffs(self):
        '''Inefficient comparison of anc and des dicts.
        Recurses through dict and lists.

        '''
        self._clear_diff_related_data()
        a = self.anc_blob
        d = self.des_blob
        context = NexsonDiffAddress()
        self._calculate_generic_diffs(a, d, self.top_skip_dict, context)

    def _process_ordering_pair(self,
                               order_key,
                               by_id_key,
                               nickname,
                               src,
                               dest,
                               address):
        d = {} if (dest is None) else dest
        s = {} if (src is None) else src
        r = _process_order_list_and_dict(order_key, by_id_key, s, d)
        if r:
            order_address = address
            r['key_order_address'] = address.ordering_child(order_key)
            r['by_id_address'] = address.by_id_child(by_id_key)
            self.add(ElementOrderDiff(nickname=nickname, **r))

    def _no_op_handling(self, src, dest, skip_dict, context, key_in_par):
        return False, None

    def _handle_nexml_diffs(self, src, dest, skip_dict, context, key_in_par):
        sc = context.child('nexml')
        self._process_ordering_pair('^ot:otusElementOrder',
                                    'otusById',
                                    nickname='otus',
                                    src=src,
                                    dest=dest,
                                    address=sc)
        self._process_ordering_pair('^ot:treesElementOrder',
                                    'treesById',
                                    nickname='trees',
                                    src=src,
                                    dest=dest,
                                    address=sc)
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
        for trees_id, s_trees in src.items():
            tsid_context = sub_context.child(trees_id)
            d_trees = dest.get(trees_id)
            if d_trees is not None:
                self._process_ordering_pair('^ot:treeElementOrder',
                                            'treeById',
                                            nickname=trees_id,
                                            src=s_trees,
                                            dest=d_trees,
                                            address=tsid_context)
                self._calculate_generic_diffs(s_trees, d_trees, trees_skip_d, tsid_context)
            else:
                self.add(DeletionDiff(s_trees, context=tsid_context))
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
        t_context = None
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
            #_LOG.debug('s_edges = {}'.format(s_edges))
            #_LOG.debug('d_edges = {}'.format(d_edges))
            for eid, s_edge in s_edges.items():
                #_LOG.debug('eid = {}'.format(eid))
                d_edge = d_edges.get(eid)
                assert d_edge is not None
                if d_edge != s_edge:
                    e_context = sub_context.child(eid)
                    self._calculate_generic_diffs(s_edge, d_edge, edge_skip, e_context)


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
            #_LOG.debug("s_extra_node_id = {}".format(s_extra_node_id))
            if (len(s_extra_node_id) > 1) or (s_extra_node_id.pop() != s_root_id):
                node_number_except = True
        d_extra_node_id = d_node_id_set - s_node_id_set
        if d_extra_node_id:
            #_LOG.debug("d_extra_node_id = {}".format(d_extra_node_id))
            if (len(d_extra_node_id) > 1) or (d_extra_node_id.pop() != d_root_id):
                node_number_except = True
        #TODO: do we need more flexible diffs. In normal curation, there can
        #   only be one node getting added (the root)
        if node_number_except:
            raise ValueError('At most one node and one edge can be added to a tree')
        t_context = None
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
                        self._calculate_generic_diffs(s_edge, d_edge, edge_skip, e_context)
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
                self.add(RerootingDiff(reroot_info, t_context))
            else:
                assert reroot_info['del_node'] is None
                assert reroot_info['del_edge'] is None
                assert reroot_info['add_node_id'] is None
                assert reroot_info['add_edge_id'] is None

    def add(self, d_obj):
        extract_by_diff_type(self.diff_dict, d_obj).append(d_obj)

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
                            if isinstance(v, list) or isinstance(dv, list) or (k in _SET_LIKE_PROPERTIES):
                                if not isinstance(v, list):
                                    v = [v]
                                if not isinstance(dv, list):
                                    dv = [dv]
                                if k in _SET_LIKE_PROPERTIES:
                                    add_type, del_type = SetAddDiff, SetDelDiff
                                    dvs = set(dv)
                                    svs = set(v)
                                    dels = svs - dvs
                                    adds = dvs - svs
                                    if adds or dels:
                                        sub_context = context.set_like_list_child(k)
                                elif k in _BY_ID_LIST_PROPERTIES:
                                    add_type, del_type = ByIdAddDiff, ByIdDelDiff
                                    dd = {i['@id']:i for i in dv}
                                    sd = {i['@id']:i for i in v}
                                    sds = set(sd.keys())
                                    dds = set(dd.keys())
                                    #_LOG.debug('sds = {}, dds = {}'.format(sds, dds))
                                    adds = dds - sds
                                    dels = sds - dds
                                    if adds:
                                        adds = tuple([dd[i] for i in adds])
                                    #_LOG.debug('adds = {}, dels = {}'.format(adds, dels))
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
                                    add_type, del_type = NoModAddDiff, NoModDelDiff
                                    # treat like _NO_MOD_PROPERTIES
                                    # ugh not efficient...
                                    dels, adds = detect_no_mod_list_dels_adds(v, dv)
                                    if adds or dels:
                                        sub_context = context.no_mod_list_child(k)
                                if dels:
                                    self.add(del_type(dels, address=sub_context))
                                if adds:
                                    self.add(add_type(adds, address=sub_context))
                                
                            else:
                                sub_context = context.child(k)
                                self.add(ModificationDiff(dv, address=sub_context))
                else:
                    if sub_context is None:
                        sub_context = context.child(k)
                    #_LOG.debug('del key "{}"'.format(k))
                    self.add(DeletionDiff(v, address=sub_context))
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
                        add_type = SetAddDiff
                    else:
                        sub_context = context.child(k)
                        add_type = AdditionDiff
                #_LOG.debug('add key "{}" from "{}"'.format(k, dest[k]))
                self.add(add_type(dest[k], address=sub_context))
