 #!/usr/bin/env python
'''Code for holding the address of nexson diff element,
finding the container for that diff in another nexson blob,  and applying a patch..
'''
from peyotl.nexson_syntax import invert_edge_by_source
from peyotl.utility import get_logger
import itertools
import json
import copy

_LOG = get_logger(__name__)

class NexsonDiffAddress(object):
    def __init__(self, par=None, key_in_par=None):
        self.par = par
        self.key_in_par = key_in_par
        self._as_ot_dict = None
        self._mb_cache = {}
    def ordering_child(self, key):
        return KeyOrderingElementAddress(self, key)
    def by_id_child(self, key):
        return ByIdOrderingElementAddress(self, key)
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
    def as_path_syntax(self):
        if self.par:
            return '{}/{}'.format(self.par.as_path_syntax(), self.key_in_par)
        return '/{}'.format(self.key_in_par)
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
            #    #_LOG.debug('self.key_in_par = {}, par_target.keys() = {}'.format(self.key_in_par, par_target.keys()))
            assert isinstance(par_target, dict) or isinstance(par_target, IDListAsDictWrapper)
            target = par_target.get(self.key_in_par)
            #if self.key_in_par == '^ot:agents':
            #    #_LOG.debug('par_target[{}] = {}'.format(self.key_in_par, target))
            if target is not None:
                self._mb_cache[ib] = target
        #else:
        #   #_LOG.debug('cache hit returning "{}"'.format(target))
        return target

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
        #_LOG.debug('ByIdListNexsonDiffAddress._find_el_in_mod_blob')
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



class SetLikeListNexsonDiffAddress(NexsonDiffAddress):
    '''Works on lists that are to be treated as sets.
    Requires that the elements are hashable. If that cannot be guaranteed a NoModListNexsonDiffAddress
    should be used.
    '''
    def __init__(self, par=None, key_in_par=None):
        NexsonDiffAddress.__init__(self, par, key_in_par)

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

    def _try_apply_mod_to_par_target(self, nexson_diff, par_target, value, blob_id):
        assert False, 'It is called NoModListNexsonDiffAddress for a reason'

class TreeNexsonDiffAddress(NexsonDiffAddress):
    def __init__(self, par=None, key_in_par=None):
        NexsonDiffAddress.__init__(self, par, key_in_par)
        self._blob_to_edge_dict = {}
        self._blob_to_target2id = {}
    def edge_child(self):
        return TreeEdgeNexsonDiffAddress(self)

class TreeEdgeNexsonDiffAddress(NexsonDiffAddress):
    def __init__(self, par=None):
        NexsonDiffAddress.__init__(self, par, 'pseudo-edge-dict')
        self.edge_by_id = None
    def _find_el_in_mod_blob(self, blob):
        assert self.par is not None
        ib = id(blob)
        target = self._mb_cache.get(ib)
        if target is None:
            #_LOG.debug('cache miss')
            if self._mb_cache:
                self._mb_cache = {}

            #_LOG.debug('Calling  NexsonDiffAddress._find_par_el_in_mod_blob from self.key_in_par = {}'.format(self.key_in_par))
            par_target = self._find_par_el_in_mod_blob(blob)
            ebs = par_target['edgeBySourceId']
            edge_dict, target2id = invert_edge_by_source(ebs)
            self._mb_cache[ib] = edge_dict
            target = edge_dict
        return target

class ByIdOrderingElementAddress(NexsonDiffAddress):
    def __init__(self, par, key_in_par):
        NexsonDiffAddress.__init__(self, par, key_in_par)

class KeyOrderingElementAddress(NexsonDiffAddress):
    def __init__(self, par, key_in_par):
        NexsonDiffAddress.__init__(self, par, key_in_par)
