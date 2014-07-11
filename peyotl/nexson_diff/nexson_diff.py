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
        rc = self._try_patch_mod_blob(blob)
        if rc == PatchRC.SUCCESS:
            return True
        if rc == PatchRC.NOT_APPLIED:
            patch_log.mark_unapplied(self)
        elif rc == PatchRC.REDUNDANT:
            patch_log.mark_redundant(self)
        return False
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
            return False
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
            return PatchRC.REDUNDANT
        del_node_id = reroot_info['del_node_id']
        if (del_node_id and (target_root != del_node_id)) \
           or ((not del_node_id) and len(ebs[target_root]) > 2):
            return PatchRC.NOT_APPLIED
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
        return PatchRC.SUCCESS

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
            return PatchRC.REDUNDANT
        return self._try_apply_del_to_par_target(par_target)
    def _try_apply_del_to_par_target(self, par_target):
        address = self.address
        assert isinstance(par_target, dict)
        if address.key_in_par in par_target:
            del par_target[address.key_in_par]
            address._mb_cache = {}
            return PatchRC.SUCCESS
        return PatchRC.REDUNDANT

class NoModDelDiff(DeletionDiff):
    def _try_apply_del_to_par_target(self, par_target):
        address = self.address
        target = par_target.get(address.key_in_par)
        if target is None:
            return PatchRC.REDUNDANT
        inds_to_del = set()
        num_not_applied = 0
        for doomed_el in self.value:
            found = False
            for n, el in enumerate(target):
                if el == doomed_el:
                    inds_to_del.add(n)
                    found = True
                    break
            if not found:
                num_not_applied += 1
        if num_not_applied == len(self.value):
            return PatchRC.REDUNDANT
        inds_to_del = list(inds_to_del)
        inds_to_del.sort(reverse=True)
        for n in inds_to_del:
            target.pop(n)
        return PatchRC.SUCCESS

class AdditionDiff(NexsonDiff):
    def __init__(self, value, address):
        NexsonDiff.__init__(self, address=address, value=value)
    def _try_patch_mod_blob(self, blob):
        a = self.address
        v = self.value
        rc = self._try_apply_add_to_mod_blob(blob, v)
        return rc

    def _try_apply_add_to_mod_blob(self, blob, value):
        '''Returns ("value in blob", "presence of key makes this addition a modification")
        records _redundant_edits
        was_mod should be true if the diff was originally a modification
        '''
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
                return PatchRC.REDUNDANT
            else:
                return PatchRC.NOT_APPLIED
        par_target[address.key_in_par] = value
        return PatchRC.SUCCESS

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
        target = par_target.get(address.key_in_par)
        if target is None:
            return PatchRC.NOT_APPLIED
        value = self.value
        num_not_applied = 0
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
            if found:
                num_not_applied += 1
            else:
                target.append(ael)
        s = [(i['@id'], i) for i in target]
        s.sort()
        del target[:]
        target.extend([i[1] for i in s])
        if num_not_applied > 0:
            return PatchRC.NOT_APPLIED
        return PatchRC.SUCCESS

class NoModAddDiff(AdditionDiff):
    def _try_apply_add_to_par_target(self, par_target):
        address = self.address
        target = par_target.get(address.key_in_par)
        if target is None:
            target = []
            par_target[address.key_in_par] = target
        if not (isinstance(target, list) or isinstance(target, tuple)):
            target = [target]
            par_target[address.key_in_par] = target
        num_not_applied = 0
        for ael in self.value:
            found = False
            for n, el in enumerate(target):
                if el == ael:
                    found = True
                    break
            if not found:
                target.append(ael)
            else:
                num_not_applied += 1
        if num_not_applied == len(self.value):
            return PatchRC.REDUNDANT
        return PatchRC.SUCCESS

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
            return PatchRC.NOT_APPLIED
        if par_target.get(address.key_in_par) == value:
            return PatchRC.REDUNDANT
        par_target[address.key_in_par] = value
        address._mb_cache = {}
        return PatchRC.SUCCESS


class SetAddDiff(AdditionDiff):
    pass

class SetDelDiff(DeletionDiff):
    pass

class ByIdDelDiff(DeletionDiff):
    def _try_apply_del_to_par_target(self, par_target):
        address = self.address
        target = par_target.get(address.key_in_par)
        if target is None:
            PatchRC.REDUNDANT
        inds_to_del = set()
        num_not_found = 0
        for kid in self.value:
            found = False
            for n, el in enumerate(target):
                if el['@id'] == kid:
                    inds_to_del.add(n)
                    found = True
                    break
            if not found:
                num_not_found += 1
        if num_not_found == len(self.value):
            return PatchRC.REDUNDANT
        inds_to_del = list(inds_to_del)
        inds_to_del.sort(reverse=True)
        for n in inds_to_del:
            target.pop(n)
        #_LOG.debug('after target = {}'.format(target))
        return PatchRC.SUCCESS

class ElementOrderDiff(NexsonDiff):
    def __init__(self,
                 address,
                 order_property=None,
                 storage_key=None,
                 by_id_property=None,
                 dest_order=None,
                 orig_order_of_retained=None,
                 added_id_map=None,
                 deleted_id_set=None):
        NexsonDiff.__init__(self, address=address, value=None)
        self.order_property = order_property
        self.storage_key = storage_key
        self.by_id_property = by_id_property
        self.dest_order = dest_order
        self.orig_order_of_retained = orig_order_of_retained
        self.added_id_map = added_id_map
        self.deleted_id_set = deleted_id_set

