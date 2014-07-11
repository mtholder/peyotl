 #!/usr/bin/env python
'''Functions for diffing and patching nexson blobs

'''
from peyotl.nexson_syntax import invert_edge_by_source
from peyotl.nexson_syntax import detect_nexson_version, \
                                 read_as_json, \
                                 write_as_json, \
                                 _is_by_id_hbf, \
                                 edge_by_source_to_edge_dict
from peyotl.nexson_diff.nexson_diff_address import NexsonDiffAddress
from peyotl.nexson_diff.patch_log import PatchReason
from peyotl.utility import get_logger
import itertools
import copy
import json

_LOG = get_logger(__name__)

class PatchRC:
    SUCCESS, REDUNDANT, NOT_APPLIED = 0, 1, 2

class NexsonDiff(object):
    def __init__(self, address, value=None):
        self.address = address
        self.value = value
    def patch_mod_blob(self, blob, patch_log):
        rd = self._try_patch_mod_blob(blob)
        u = rd.get(PatchRC.NOT_APPLIED)
        if u is not None:
            if not isinstance(u, list):
                u = [u]
            for i in u:
                diff, reason = i
                patch_log.mark_unapplied(diff, reason)

        r = rd.get(PatchRC.REDUNDANT)
        if r is not None:
            if not isinstance(r, list):
                r = [r]
            for i in r:
                diff, reason = i
                patch_log.mark_redundant(diff, reason)
        return PatchRC.SUCCESS in rd
    def as_ot_diff(self):
        v = copy.deepcopy(self.value)
        if isinstance(v, set):
            vl = list(v)
            vl.sort()
            y = {'value': vl}
        else:
            y = {'value': v}
        if self.address is not None:
            y.update(self.address.as_ot_target())
        return y
    def _redundant_return(self):
        return {PatchRC.REDUNDANT: (self, PatchReason.REDUNDANT)}
    def _success_return(self):
        return {PatchRC.SUCCESS: (self, PatchReason.SUCCESS)}
    def _not_applied_return(self, reason):
        return {PatchRC.NOT_APPLIED: (self, reason)}
class RerootingDiff(NexsonDiff):
    def __init__(self, reroot_info, address):
        NexsonDiff.__init__(self, address=address, value=reroot_info)
    def _try_patch_mod_blob(self, blob):
        return self.try_apply_rerooting_to_mod_blob(blob)
    def try_apply_rerooting_to_mod_blob(self,
                                        base_blob):
        reroot_info = self.value
        address = self.address
        par_target = address._find_par_el_in_mod_blob(base_blob)
        #_LOG.debug('mod call on address.key_in_par = "{}" to "{}" applied to par_target="{}"'.format(address.key_in_par, value, par_target))
        target = par_target.get(address.key_in_par)
        if target is None:
            return {PatchRC.NOT_APPLIED: (self, PatchReason.CONTAINER_GONE)}
        #_LOG.debug('nexson_diff.__dict__ =' + str(nexson_diff.__dict__))
        bib = id(base_blob)
        edge_dict = address._blob_to_edge_dict.get(bib)
        ebs = target['edgeBySourceId']
        if edge_dict is None:
            address._blob_to_edge_dict = {}
            edge_dict, target2id = invert_edge_by_source(ebs)
            address._blob_to_edge_dict[bib] = edge_dict
            address._blob_to_target2id[bib] = target2id
        else:
            target2id = address._blob_to_target2id[bib]
        target_root = target['^ot:rootNodeId']

        new_root_id = reroot_info['new_root_id']
        if target_root == new_root_id:
            return self._redundant_return()
        del_node_id = reroot_info['del_node_id']
        if (del_node_id and (target_root != del_node_id)) \
           or ((not del_node_id) and len(ebs[target_root]) > 2):
            return {PatchRC.NOT_APPLIED: (self, PatchReason.TOO_DIFFERENT)}
        etd_id = reroot_info['del_edge_id']
        del_node_id = reroot_info['del_node_id']
        if del_node_id:
            del_node = target['nodeById'][del_node_id]
        if etd_id:
            assert del_node_id
            edge_to_del = ebs[del_node_id][etd_id]
        eta_id = reroot_info['add_edge_id']
        edge_to_add = reroot_info['add_edge']
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
            #_LOG.debug('No edge to add')
            assert(new_root_id in target2id)
            etf_id = target2id[new_root_id]
            edge_to_flip = edge_dict[etf_id]
        else:
            #_LOG.debug('add_edge_sib_edge_id = {}'.format(add_edge_sib_edge_id))
            assert(eta_id not in edge_dict)
            target_of_eta_id = edge_to_add['@target']
            assert add_edge_sib_edge_id is not None
            add_edge_sib_edge = ebs[target_of_eta_id][add_edge_sib_edge_id]
            sib_of_target_id = add_edge_sib_edge['@target']
            #_LOG.debug('target_of_eta_id = {}'.format(target_of_eta_id))
            #_LOG.debug('sib_of_target_id = {}'.format(sib_of_target_id))
            assert add_edge_sib_edge_id in ebs[target_of_eta_id]
            del ebs[target_of_eta_id][add_edge_sib_edge_id]
            ebs[target_of_eta_id][eta_id] = copy.deepcopy(edge_to_add)
            ebs[nta_id] = {add_edge_sib_edge_id: add_edge_sib_edge,}
            add_edge_sib_edge['@source'] = nta_id
            edge_to_flip = ebs[target_of_eta_id][eta_id]
            etf_id = eta_id
            # flip it, so that it is backward for the iterated of flipping
            #   seems stupid, but reduces code duplication below
            edge_to_flip['@target'], edge_to_flip['@source'] = edge_to_flip['@source'], edge_to_flip['@target'],
        #_LOG.debug('edge_dict = {}'.format(edge_dict))
        #_LOG.debug('nri = {}\ndel = {}\nadd = {}'.format(new_root_id, etd_id, eta_id))
        while edge_to_flip is not None:

            _target, _source = edge_to_flip['@target'], edge_to_flip['@source']
            #_LOG.debug("edge_to_flip t,s = {}, {}".format(_target, _source))
            if etf_id != etd_id:
                del ebs[_source][etf_id]
            _target, _source = _source, _target
            ebs[_source][etf_id] = edge_to_flip
            edge_to_flip['@target'], edge_to_flip['@source'] = _target, _source
            already_flipped.add(etf_id)
            etf_id = target2id.get(_target)
            #_LOG.debug("etf_id  {}".format(etf_id))
            if (etf_id is None) or (etf_id in already_flipped):
                break
            edge_to_flip = edge_dict[etf_id]
        #_LOG.debug('ebs = ' + str(ebs))
        if del_node_id:
            #_LOG.debug('del_node_id = ' + del_node_id)
            #_LOG.debug('etd_id = ' + etd_id)
            edges_from_doomed_root = ebs[del_node_id]
            assert len(edges_from_doomed_root) == 2
            sib_to_grow_id, sib_to_grow = None, None
            for ei, eo in edges_from_doomed_root.items():
                if ei != etd_id:
                    sib_to_grow_id, sib_to_grow = ei, eo
                    break
            #_LOG.debug('ebs[del_node_id] = ' + str(edges_from_doomed_root))
            #_LOG.debug('edge_to_del = ' + str(edge_to_del))
            #_LOG.debug('sib_to_grow = ' + str(sib_to_grow))
            if etd_id in already_flipped:
                _real_source = edge_to_del['@source']
                assert edge_to_del['@target'] == del_node_id
                _real_target = sib_to_grow['@target']
                assert sib_to_grow['@source'] == del_node_id
                del ebs[_real_source][etd_id]
            else:
                assert sib_to_grow_id is already_flipped
                _real_source = sib_to_grow['@source']
                assert sib_to_grow['@target'] == del_node_id
                _real_target = edge_to_del['@target']
                assert edge_to_del['@source'] == del_node_id
            del ebs[del_node_id]
            sib_to_grow['@source'], sib_to_grow['@target'] = _real_source, _real_target
            ebs[_real_source][sib_to_grow_id] = sib_to_grow
            del target['nodeById'][del_node_id]
        if nta_id:
            assert nta_id in ebs # we added it above in the flipping code...
            target['nodeById'][nta_id] = copy.deepcopy(nd_to_add)
        return self._success_return()

class DeletionDiff(NexsonDiff):
    def __init__(self, value, address):
        NexsonDiff.__init__(self, address=address, value=value)
    def _try_patch_mod_blob(self, blob):
        return self._try_apply_del_to_mod_blob(blob)
    def _try_apply_del_to_mod_blob(self, blob):
        address = self.address
        assert address.par is not None # can't delete the whole blob!
        par_target = address._find_par_el_in_mod_blob(blob)
        if par_target is None:
            return self._redundant_return()
        return self._try_apply_del_to_par_target(par_target)
    def _try_apply_del_to_par_target(self, par_target):
        address = self.address
        assert isinstance(par_target, dict)
        if address.key_in_par in par_target:
            del par_target[address.key_in_par]
            address._mb_cache = {}
            return self._success_return()
        return self._redundant_return()

class NoModDelDiff(DeletionDiff):
    def _try_apply_del_to_par_target(self, par_target):
        address = self.address
        target = par_target.get(address.key_in_par)
        if target is None:
            return self._redundant_return()
        inds_to_del = set()
        not_applied, applied = [], []
        for doomed_el in self.value:
            found = False
            for n, el in enumerate(target):
                if el == doomed_el:
                    inds_to_del.add(n)
                    found = True
                    break
            if not found:
                not_applied.append(doomed_el)
            else:
                applied.append(doomed_el)
        r = {}
        if not_applied:
            nmdd = NoModDelDiff(address=self.address, value=not_applied)
            r[PatchRC.NOT_APPLIED] = (nmdd, PatchReason.ELEMENT_GONE)
        if len(not_applied) < len(self.value):
            inds_to_del = list(inds_to_del)
            inds_to_del.sort(reverse=True)
            for n in inds_to_del:
                target.pop(n)
            nmdd = NoModDelDiff(address=self.address, value=applied)
            r[PatchRC.SUCCESS] = (nmdd, PatchReason.SUCCESS)
        return r

class AdditionDiff(NexsonDiff):
    def __init__(self, value, address):
        NexsonDiff.__init__(self, address=address, value=value)
    def _try_patch_mod_blob(self, blob):
        a = self.address
        v = self.value
        return self._try_apply_add_to_mod_blob(blob, v)

    def _try_apply_add_to_mod_blob(self, blob, value):
        address = self.address
        par_target = address._find_par_el_in_mod_blob(blob)
        assert par_target is not None
        return self._try_apply_add_to_par_target(par_target)

    def _try_apply_add_to_par_target(self, par_target):
        address = self.address
        value = self.value
        assert isinstance(par_target, dict)
        if address.key_in_par in par_target:
            pv = par_target[address.key_in_par]
            if pv == value:
                return self._redundant_return()
            return self._not_applied_return(PatchReason.EDITED)
        par_target[address.key_in_par] = value
        address._mb_cache = {}
        return self._success_return()

class ByIdAddDiff(AdditionDiff):
    def _try_apply_add_to_par_target(self, par_target):
        #_LOG.debug('_try_apply_add_to_par_target')
        address = self.address
        assert isinstance(par_target, dict)
        if address.key_in_par in par_target:
            pv = par_target[address.key_in_par]
            if not isinstance(pv, list) or isinstance(pv, tuple):
                pv = [pv]
                par_target[address.key_in_par] = pv
                address._mb_cache = {}
        target = par_target.get(address.key_in_par)
        if target is None:
            return self._not_applied_return(PatchReason.CONTAINER_GONE)
        value = self.value
        not_applied, applied = [], []
        for ael in value:
            found = False
            kid = ael['@id']
            for n, el in enumerate(target):
                if el['@id'] == kid:
                    found = True
                    break
            if found:
                not_applied.append(ael)
            else:
                applied.append(ael)
                target.append(ael)
        r = {}
        if not_applied:
            nadd = ByIdAddDiff(address=self.address, value=not_applied)
            r[PatchRC.NOT_APPLIED] = (nadd, PatchReason.ELEMENT_GONE)
        if len(applied):
            s = [(i['@id'], i) for i in target]
            s.sort()
            del target[:]
            target.extend([i[1] for i in s])
            nadd = ByIdAddDiff(address=self.address, value=applied)
            r[PatchRC.SUCCESS] = (nadd, PatchReason.SUCCESS)
        return r

class NoModAddDiff(AdditionDiff):
    def _try_apply_add_to_par_target(self, par_target):
        address = self.address
        target = par_target.get(address.key_in_par)
        if target is None:
            target = []
            par_target[address.key_in_par] = target
            address._mb_cache = {}
        if not (isinstance(target, list) or isinstance(target, tuple)):
            target = [target]
            par_target[address.key_in_par] = target
            address._mb_cache = {}
        not_applied, applied = [], []
        for ael in self.value:
            found = False
            for n, el in enumerate(target):
                if el == ael:
                    found = True
                    break
            if not found:
                applied.append(ael)
                target.append(ael)
            else:
                not_applied.append(ael)
        r = {}
        if not_applied:
            nmdd = NoModAddDiff(address=self.address, value=not_applied)
            r[PatchRC.NOT_APPLIED] = (nmdd, PatchReason.ELEMENT_GONE)
        if len(not_applied) < len(self.value):
            nmdd = NoModAddDiff(address=self.address, value=applied)
            r[PatchRC.SUCCESS] = (nmdd, PatchReason.SUCCESS)
        return r

class ModificationDiff(NexsonDiff):
    def __init__(self, value, address):
        #_LOG.debug('ModificationDiff at {} value={}'.format(address.as_path_syntax(), value))
        NexsonDiff.__init__(self, address=address, value=value)
    def _try_patch_mod_blob(self, blob):
        a = self.address
        v = self.value
        return self._try_apply_mod_to_mod_blob(blob, v)
    def _try_apply_mod_to_mod_blob(self, blob, value):
        address = self.address
        par_target = address._find_par_el_in_mod_blob(blob)
        #_LOG.debug('Mod patch apply: par_target = '.format(par_target))
        assert par_target is not None
        if address.key_in_par not in par_target:
            return self._not_applied_return(PatchReason.ELEMENT_GONE)
        if par_target.get(address.key_in_par) == value:
            return self._redundant_return()
        par_target[address.key_in_par] = value
        address._mb_cache = {}
        return self._success_return()


class SetAddDiff(AdditionDiff):
    pass

class SetDelDiff(DeletionDiff):
    pass

class ByIdDelDiff(DeletionDiff):
    def _try_apply_del_to_par_target(self, par_target):
        address = self.address
        target = par_target.get(address.key_in_par)
        if target is None:
            return self._redundant_return()
        inds_to_del = set()
        not_applied, applied = [], []
        for kid in self.value:
            found = False
            for n, el in enumerate(target):
                if el['@id'] == kid:
                    inds_to_del.add(n)
                    found = True
                    break
            if not found:
                not_applied.append(kid)
            else:
                applied.append(kid)
        r = {}
        if not_applied:
            nmdd = ByIdDelDiff(address=self.address, value=not_applied)
            r[PatchRC.REDUNDANT] = (nmdd, PatchReason.REDUNDANT)
        if len(not_applied) < len(self.value):
            inds_to_del = list(inds_to_del)
            inds_to_del.sort(reverse=True)
            for n in inds_to_del:
                target.pop(n)
            nmdd = ByIdDelDiff(address=self.address, value=applied)
            r[PatchRC.SUCCESS] = (nmdd, PatchReason.SUCCESS)
        return r

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

class ElementOrderDiff(NexsonDiff):
    def __init__(self,
                 key_order_address=None,
                 nickname=None,
                 by_id_address=None,
                 dest_order=None,
                 orig_order_of_retained=None,
                 added_id_map=None,
                 deleted_id_set=None):
        NexsonDiff.__init__(self, address=None, value=None)
        self.key_order_address = key_order_address
        self.nickname = nickname
        self.by_id_address = by_id_address
        self.dest_order = dest_order
        self.orig_order_of_retained = orig_order_of_retained
        self.added_id_map = added_id_map
        if not added_id_map:
            self.added_id_map = {}
        self.deleted_id_set = deleted_id_set
        if not deleted_id_set:
            self.deleted_id_set = set()
    def _try_patch_mod_blob(self, blob):
        par_target = self.by_id_address._find_par_el_in_mod_blob(blob)
        if par_target is None:
            return self._not_applied_return(PatchReason.CONTAINER_GONE)
        by_id_coll = par_target.get(self.by_id_address.key_in_par)
        if by_id_coll is None:
            return self._not_applied_return(PatchReason.ELEMENT_GONE)
        order_par_target = self.key_order_address._find_par_el_in_mod_blob(blob)
        if order_par_target is None:
            return self._not_applied_return(PatchReason.CONTAINER_GONE)
        order_coll = par_target.get(self.key_order_address.key_in_par)
        if order_coll is None:
            return self._not_applied_return(PatchReason.ELEMENT_GONE)
        
        base_set = set(order_coll)
        applied_del, not_applied_del = [], []
        for k in self.deleted_id_set:
            if k not in order_coll:
                #_LOG.debug('already deleted ' + k)
                not_applied_del.append(k)
            else:
                del by_id_coll[k]
                applied_del.append(k)
                order_coll.remove(k)
        added = set()
        added_order = []
        applied_add, not_applied_add, red_add = {}, {}, {}
        for k, v in self.added_id_map.items():
            if k in order_coll:
                if v == order_coll[k]:
                    red_add[k] = v
                else:
                    not_applied_add[k] = v
            else:
                by_id_coll[k] = v
                applied_add[k] = v
                added_order.append(k)
                added.add(k)

        #TODO: better ordering when both have changed?
        orig_set = set(self.orig_order_of_retained)
        edit_set = set(self.dest_order)
        in_all_three = edit_set.intersection(base_set).intersection(orig_set)
        orig_order_3 = [i for i in self.orig_order_of_retained if i in in_all_three]
        base_order_3 = [i for i in order_coll if i in in_all_three]
        edit_order_3 = [i for i in self.dest_order if i in in_all_three]
        if (orig_order_3 == base_order_3) and(orig_order_3 != edit_order_3):
            final_order = edit_order_3
        else:
            final_order = base_order_3
        added_in_base = (set(order_coll)  - added) - in_all_three
        if added_in_base:
            added_in_base_order = [i for i in order_coll if i in added_in_base]
            _add_in_order(final_order, added_in_base_order, order_coll, in_all_three)
        if added:
            _add_in_order(final_order, added_order, self.dest_order, in_all_three)
        # Set the order property...
        order_par_target[self.key_order_address.key_in_par] = final_order
        self.key_order_address._mb_cache = {}
        r = {}
        if not_applied_del or red_add:
            nmdd = ElementOrderDiff(key_order_address=self.key_order_address,
                                    nickname=self.nickname,
                                    by_id_address=self.by_id_address,
                                    dest_order=self.dest_order,
                                    orig_order_of_retained=self.orig_order_of_retained,
                                    added_id_map=red_add,
                                    deleted_id_set=not_applied_del)
            r[PatchRC.REDUNDANT] = (nmdd, PatchReason.REDUNDANT)
        if not_applied_add:
            nmdd = ElementOrderDiff(key_order_address=self.key_order_address,
                                    nickname=self.nickname,
                                    by_id_address=self.by_id_address,
                                    dest_order=self.dest_order,
                                    orig_order_of_retained=self.orig_order_of_retained,
                                    added_id_map=not_applied_add,
                                    deleted_id_set=None)
            r[PatchRC.NOT_APPLIED] = (nmdd, PatchReason.EDITED)
        if applied_add or applied_del:
            nmdd = ElementOrderDiff(key_order_address=self.key_order_address,
                                    nickname=self.nickname,
                                    by_id_address=self.by_id_address,
                                    dest_order=self.dest_order,
                                    orig_order_of_retained=self.orig_order_of_retained,
                                    added_id_map=applied_add,
                                    deleted_id_set=applied_del)
            r[PatchRC.SUCCESS] = (nmdd, PatchReason.SUCCESS)
        return r
