 #!/usr/bin/env python
'''Code for holding the address of nexson diff element,
finding the container for that diff in another nexson blob,  and applying a patch..
'''
from peyotl.nexson_syntax import invert_edge_by_source
from peyotl.struct_diff import DictDiff
from peyotl.utility import get_logger
import itertools
import json

_LOG = get_logger(__name__)

def _del_merge_set_like_properties(curr_v, to_del):
    if not (isinstance(to_del, list) or isinstance(to_del, tuple)):
        to_del = [to_del]
    if not (isinstance(curr_v, list) or isinstance(curr_v, tuple)):
        curr_v = [curr_v]
    all_v = set(curr_v) - set(to_del)
    all_v_l = list(all_v)
    all_v_l.sort()
    return all_v_l


def _merge_set_like_properties(value, pv):
    if not (isinstance(value, list) or isinstance(value, tuple)):
        value = [value]
    if not (isinstance(pv, list) or isinstance(pv, tuple)):
        pv = [pv]
    all_v = set(value).union(set(pv))
    all_v_l = list(all_v)
    all_v_l.sort()
    return all_v_l


class NexsonDiffAddress(object):
    def __init__(self, par=None, key_in_par=None):
        self.par = par
        self.key_in_par = key_in_par
        self._as_ot_dict = None
        self._mb_cache = {}

    def child(self, key_in_par):
        #_LOG.debug('id={} NexsonDiffAddress.child({})'.format(id(self), key_in_par))
        return NexsonDiffAddress(par=self, key_in_par=key_in_par)

    def by_id_list_child(self, key_in_par):
        return ByIdListNexsonDiffAddress(self, key_in_par)

    def set_like_list_child(self, key_in_par):
        return SetLikeListNexsonDiffAddress(self, key_in_par)

    def no_mod_list_child(self, key_in_par):
        return NoModListNexsonDiffAddress(self, key_in_par)

    def create_tree_context(self):
        return TreeNexsonDiffAddress(self.par, self.key_in_par)

    def as_ot_target(self):
        if self._as_ot_dict is None:
            if self.par is None:
                if self.key_in_par is None:
                    self._as_ot_dict = {'address': tuple()}
                else:
                    self._as_ot_dict = {'address': (self.key_in_par,)}
            else:
                assert self.key_in_par is not None
                pl = [i for i in self.par.as_ot_target()['address']]
                pl.append(self.key_in_par)
                self._as_ot_dict = {'address': tuple(pl)}
        return self._as_ot_dict

    def _find_par_el_in_mod_blob(self, blob):
        return self.par._find_el_in_mod_blob(blob)

    def _find_el_in_mod_blob(self, blob):
        if self.par is None:
            #_LOG.debug('_find_el_in_mod_blob parentless id = {}'.format(id(self)))
            return blob
        ib = id(blob)
        target = self._mb_cache.get(ib)
        if target is None:
            #_LOG.debug('cache miss')
            if self._mb_cache:
                self._mb_cache = {}
            #_LOG.debug('Calling  NexsonDiffAddress._find_par_el_in_mod_blob from self.key_in_par = {}'.format(self.key_in_par))
            par_target = self._find_par_el_in_mod_blob(blob)
            #if self.key_in_par == '^ot:agents':
            #    _LOG.debug('self.key_in_par = {}, par_target.keys() = {}'.format(self.key_in_par, par_target.keys()))
            assert isinstance(par_target, dict) or isinstance(par_target, IDListAsDictWrapper)
            target = par_target.get(self.key_in_par)
            #if self.key_in_par == '^ot:agents':
            #    _LOG.debug('par_target[{}] = {}'.format(self.key_in_par, target))
            if target is not None:
                self._mb_cache[ib] = target
        #else:
        #    _LOG.debug('cache hit returning "{}"'.format(target))
        return target

    def try_apply_del_to_mod_blob(self, nexson_diff, blob, del_v):
        assert self.par is not None # can't delete the whole blob!
        par_target = self._find_par_el_in_mod_blob(blob)
        #_LOG.debug('del call on self.key_in_par = "{}" on par_target = "{}"'.format(self.key_in_par, par_target))
        #_LOG.debug('self.key_in_par = {} par_target={}'.format(self.key_in_par, par_target))
        if par_target is None:
            return False
        assert isinstance(par_target, dict)
        return self._try_apply_del_to_par_target(nexson_diff, par_target, del_v)

    def _try_apply_del_to_par_target(self, nexson_diff, par_target, del_v):
        #_LOG.debug('par_target.keys() =' + str(par_target.keys()))
        if self.key_in_par in par_target:
            del par_target[self.key_in_par]
            self._mb_cache = {}
            return True
        #_LOG.debug('redundant del')
        nexson_diff._redundant_edits['deletions'].append((del_v, self))
        return False

    def try_apply_add_to_mod_blob(self, nexson_diff, blob, value, was_mod):
        '''Returns ("value in blob", "presence of key makes this addition a modification")
        records _redundant_edits
        was_mod should be true if the diff was originally a modification
        '''
        assert self.par is not None
        par_target = self._find_par_el_in_mod_blob(blob)
        #_LOG.debug('add call on self.key_in_par = "{}" on {}'.format(self.key_in_par, par_target))
        if par_target is None:
            return False, False
        assert isinstance(par_target, dict)

        if self.key_in_par in par_target:
            pv = par_target[self.key_in_par]
            if pv == value:
                if was_mod:
                    container = nexson_diff._redundant_edits['modifications']
                else:
                    container = nexson_diff._redundant_edits['additions']
                container.append((value, self))
                return True, True

        return self._try_apply_add_to_par_target(nexson_diff, par_target, value, was_mod, id(blob))

    def _try_apply_add_to_par_target(self, nexson_diff, par_target, value, was_mod, blob_id):
        if self.key_in_par in par_target:
            return False, True
        assert not isinstance(value, DictDiff)
        par_target[self.key_in_par] = value
        self._mb_cache = {}
        return True, True

    def try_apply_mod_to_mod_blob(self, nexson_diff, blob, value, was_add):
        par_target = self._find_par_el_in_mod_blob(blob)
        #_LOG.debug('mod call on self.key_in_par = "{}" to "{}" applied to par_target="{}"'.format(self.key_in_par, value, par_target))
        if self.key_in_par not in par_target:
            return False
        assert not isinstance(value, DictDiff)
        if par_target.get(self.key_in_par) == value:
            if was_add:
                container = nexson_diff._redundant_edits['additions']
            else:
                container = nexson_diff._redundant_edits['modifications']
            container.append((value, self))
        else:
            self._try_apply_mod_to_par_target(nexson_diff, par_target, value, id(blob))
        return True

    def _try_apply_mod_to_par_target(self, nexson_diff, par_target, value, blob_id):
        par_target[self.key_in_par] = value
        self._mb_cache = {}

class IDListAsDictWrapper(object):
    def __init__(self, idl):
        self.idl = idl
    def get(self, key):
        #_LOG.debug('IDListAsDictWrapper.get({})'.format(key))
        for el in self.idl:
            if el['@id'] == key:
                return el
        return None

class ByIdListNexsonDiffAddress(NexsonDiffAddress):
    def __init__(self, par=None, key_in_par=None):
        NexsonDiffAddress.__init__(self, par, key_in_par)
    def _find_el_in_mod_blob(self, blob):
        assert self.par is not None
        #_LOG.debug('ByIdListNexsonDiffAddress._find_el_in_mod_blob blob={}'.format(blob))
        ib = id(blob)
        target = self._mb_cache.get(ib)
        if target is None:
            #_LOG.debug('cache miss')
            if self._mb_cache:
                self._mb_cache = {}
            #_LOG.debug('Calling  ByIdListNexsonDiffAddress._find_par_el_in_mod_blob from self.key_in_par = {}'.format(self.key_in_par))
            par_target = self._find_par_el_in_mod_blob(blob)
            #_LOG.debug('self.key_in_par = {}, par_target = {}'.format(self.key_in_par, par_target))
            assert isinstance(par_target, dict)
            target = par_target.get(self.key_in_par)
            assert isinstance(target, list)
            target = IDListAsDictWrapper(target)
            #_LOG.debug('self.key_in_par = {}, target = {}'.format(self.key_in_par, target))
            if target is not None:
                self._mb_cache[ib] = target
        #_LOG.debug('ByIdListNexsonDiffAddress target =  "{}"'.format(target))
        return target
    def child(self, key_in_par):
        #_LOG.debug('ByIdListNexsonDiffAddress.key_in_par={} child.key_in_par={}'.format(self.key_in_par, key_in_par))
        return NexsonDiffAddress.child(self, key_in_par)
    def _try_apply_del_to_par_target(self, nexson_diff, par_target, del_v):
        target = par_target.get(self.key_in_par)
        if target is None:
            nexson_diff._redundant_edits['deletions'].append((del_v, self))
        #_LOG.debug('before target = {}'.format(target))
        inds_to_del = set()
        for kid in del_v:
            found = False
            #_LOG.debug('kid = {}'.format(kid))
            for n, el in enumerate(target):
                try:
                    if el['@id'] == kid:
                        inds_to_del.add(n)
                        found = True
                        break
                except:
                    raise
                    pass
            if not found:
                nexson_diff._redundant_edits['deletions'].append((kid, self))
        inds_to_del = list(inds_to_del)
        inds_to_del.sort(reverse=True)
        for n in inds_to_del:
            target.pop(n)
        #_LOG.debug('after target = {}'.format(target))
        return bool(inds_to_del)

    def _try_apply_add_to_par_target(self, nexson_diff, par_target, value, was_mod, blob_id):
        target = par_target.get(self.key_in_par)
        if target is None:
            nexson_diff._unapplied_edits['addition'].append((value, self))
        #_LOG.debug('before-add target = {}'.format(target))
        for ael in value:
            found = False
            kid = ael['@id']
            for n, el in enumerate(target):
                try:
                    if el['@id'] == kid:
                        found = True
                        break
                except:
                    raise
                    pass
            if not found:
                target.append(ael)
        s = [(i['@id'], i) for i in target]
        s.sort()
        del target[:]
        target.extend([i[1] for i in s])
        #_LOG.debug('after-add target = {}'.format(target))
        return True, True

    def _try_apply_mod_to_par_target(self, nexson_diff, par_target, value, blob_id):
        par_target[self.key_in_par] = value
        self._mb_cache = {}


class SetLikeListNexsonDiffAddress(NexsonDiffAddress):
    '''Works on lists that are to be treated as sets.
    Requires that the elements are hashable. If that cannot be guaranteed a NoModListNexsonDiffAddress
    should be used.
    '''
    def __init__(self, par=None, key_in_par=None):
        NexsonDiffAddress.__init__(self, par, key_in_par)
    def _try_apply_del_to_par_target(self, nexson_diff, par_target, del_v):
        #_LOG.debug('par_target.keys() =' + str(par_target.keys()))
        if self.key_in_par in par_target:
            nv = _del_merge_set_like_properties(par_target[self.key_in_par], del_v)
            if nv:
                par_target[self.key_in_par] = nv
            else:
                del par_target[self.key_in_par]
            #self._mb_cache = {}
            return True
        #_LOG.debug('redundant del')
        nexson_diff._redundant_edits['deletions'].append((del_v, self))
        return False

    def _try_apply_add_to_par_target(self, nexson_diff, par_target, value, was_mod, blob_id):
        if self.key_in_par in par_target:
            pv = par_target[self.key_in_par]
            all_v_l = _merge_set_like_properties(value, pv)
            par_target[self.key_in_par] = all_v_l
            #self._mb_cache = {}
            return True, True
        assert not isinstance(value, DictDiff)
        par_target[self.key_in_par] = value
        #self._mb_cache = {blob_id: value}
        return True, True

    def _try_apply_mod_to_par_target(self, nexson_diff, par_target, value, blob_id):
        all_v_l = _merge_set_like_properties(value, par_target[self.key_in_par])
        par_target[self.key_in_par] = all_v_l
        #self._mb_cache = {}

class NoModListNexsonDiffAddress(NexsonDiffAddress):
    '''Acts like the SetLikeListNexsonDiffAddress, but works with items
    that are not hashable. This makes it slow. Equality testing means that it
    is O(N*M) where N and M are the lengths of the edit list (N) and destination list.
    
    "NoMod" refers to the fact that (when the diff is being inferred)
        it is assumed that if 2 entities differ at all, they are not modifications of the
        same entity.
    '''
    def __init__(self, par=None, key_in_par=None):
        NexsonDiffAddress.__init__(self, par, key_in_par)
    def _try_apply_del_to_par_target(self, nexson_diff, par_target, del_v):
        target = par_target.get(self.key_in_par)
        if target is None:
            nexson_diff._redundant_edits['deletions'].append((del_v, self))
        #_LOG.debug('before target = {}'.format(target))
        inds_to_del = set()
        for doomed_el in del_v:
            found = False
            #_LOG.debug('kid = {}'.format(kid))
            for n, el in enumerate(target):
                try:
                    if el == doomed_el:
                        inds_to_del.add(n)
                        found = True
                        break
                except:
                    raise
                    pass
            if not found:
                nexson_diff._redundant_edits['deletions'].append((doomed_el, self))
        inds_to_del = list(inds_to_del)
        inds_to_del.sort(reverse=True)
        for n in inds_to_del:
            target.pop(n)
        #_LOG.debug('after target = {}'.format(target))
        return bool(inds_to_del)

    def _try_apply_add_to_par_target(self, nexson_diff, par_target, added_list, was_mod, blob_id):
        target = par_target.get(self.key_in_par)
        if target is None:
            nexson_diff._unapplied_edits['addition'].append((added_list, self))
        #_LOG.debug('before-add target = {}'.format(target))
        for ael in added_list:
            found = False
            for n, el in enumerate(target):
                try:
                    if el == ael:
                        found = True
                        break
                except:
                    raise
                    pass
            if not found:
                target.append(ael)
        #_LOG.debug('after-add target = {}'.format(target))
        return True, True

    def _try_apply_mod_to_par_target(self, nexson_diff, par_target, value, blob_id):
        assert False, 'It is called NoModListNexsonDiffAddress for a reason'


class TreeNexsonDiffAddress(NexsonDiffAddress):
    def __init__(self, par=None, key_in_par=None):
        NexsonDiffAddress.__init__(self, par, key_in_par)
        self._blob_to_edge_dict = {}
        self._blob_to_target2id = {}
    def try_apply_rerooting_to_mod_blob(self,
                                        nexson_diff,
                                        base_blob,
                                        reroot_info):
        par_target = self._find_par_el_in_mod_blob(base_blob)
        #_LOG.debug('mod call on self.key_in_par = "{}" to "{}" applied to par_target="{}"'.format(self.key_in_par, value, par_target))
        target = par_target.get(self.key_in_par)
        if target is None:
            return False

        bib = id(base_blob)
        edge_dict = self._blob_to_edge_dict.get(bib)
        ebs = target['edgeBySourceId']
        if edge_dict is None:
            self._blob_to_edge_dict = {}
            edge_dict, target2id = invert_edge_by_source(ebs)
            self._blob_to_edge_dict[bib] = edge_dict
            self._blob_to_target2id[bib] = target2id
        else:
            target2id = self._blob_to_target2id[bib]
        target_root = target['^ot:rootNodeId']

        new_root_id = reroot_info['new_root_id']
        if target_root == new_root_id:
            nexson_diff._redundant_edits['rerootings'].append((reroot_info, self))
            return True
        del_node_id = reroot_info['del_node_id']
        if (del_node_id and (target_root != del_node_id)) \
           or ((not del_node_id) and len(ebs[target_root]) > 2):
            nexson_diff._unapplied_edits['rerootings'].append((reroot_info, self))
            return False
        etd_id = reroot_info['del_edge_id']
        edge_to_del = reroot_info['del_edge']
        del_node = reroot_info['del_node']
        eta_id = reroot_info['add_edge_id']
        edge_to_add = reroot_info['add_edge']
        add_edge_sib_edge = reroot_info['add_edge_sib_edge']
        add_edge_sib_edge_id = reroot_info['add_edge_sib_edge_id']
        nta_id = reroot_info['add_node_id']
        nd_to_add = reroot_info['add_node']
        # Do the actual rerooting...
        target['^ot:rootNodeId'] = new_root_id
        already_flipped = set()
        if new_root_id in ebs:
            assert(nd_to_add is None)
        else:
            assert(nd_to_add is not None)
        if edge_to_add is None:
            assert(new_root_id in target2id)
            etf_id = target2id[new_root_id]
            edge_to_flip = edge_dict[etf_id]
        else:
            assert(eta_id not in edge_dict)
            target_of_eta_id = edge_to_add['@target']
            assert add_edge_sib_edge is not None
            assert add_edge_sib_edge_id is not None
            sib_of_target_id = add_edge_sib_edge['@target']
            if add_edge_sib_edge_id in ebs.get(sib_of_target_id, {}):
                edge_to_flip = ebs[sib_of_target_id][add_edge_sib_edge_id]
            else:
                assert add_edge_sib_edge_id in ebs.get(target_of_eta_id, {})
                edge_to_flip = ebs[target_of_eta_id][add_edge_sib_edge_id]
                edge_to_flip['@source'] = target_of_eta_id
                already_flipped.add(add_edge_sib_edge_id)
            etf_id = add_edge_sib_edge_id
            edge_to_flip['@target'] = nta_id
        _LOG.debug('edge_dict = {}'.format(edge_dict))
        _LOG.debug('nri = {}\ndel = {}\nadd = {}'.format(new_root_id, etd_id, eta_id))
        while edge_to_flip is not None:
            _target, _source = edge_to_flip['@target'], edge_to_flip['@source']
            print "t,s =", _target, _source
            _target, _source = _source, _target
            edge_to_flip['@target'], edge_to_flip['@source'] = _target, _source
            already_flipped.add(etf_id)
            etf_id = target2id.get(_source)
            if etf_id in already_flipped:
                break
            edge_to_flip = edge_dict[etf_id]
    def edge_child(self):
        return TreeEdgeNexsonDiffAddress(self)
class TreeEdgeNexsonDiffAddress(NexsonDiffAddress):
    def __init__(self, par=None):
        NexsonDiffAddress.__init__(self, par, None)


#nd_to_add, edge_to_add = add_nd_edge
        