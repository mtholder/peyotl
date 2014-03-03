#!/usr/bin/env python
'''NexsonValidationAdaptor class.
'''
from cStringIO import StringIO
import datetime
import codecs
import json
import re
from peyotl.nexson_validation.helper import SeverityCodes
from peyotl.nexson_validation.err_generator import factory2code, \
                                                   gen_MissingMandatoryKeyWarning, \
                                                   gen_UnrecognizedKeyWarning
from peyotl.nexson_syntax.helper import get_nexml_el, \
                                        _is_badgerfish_version, \
                                        _is_by_id_hbf, \
                                        _is_direct_hbf

from peyotl.nexson_syntax import detect_nexson_version
_NEXEL_TOP_LEVEL = 0
_NEXEL_NEXML = 1
_NEXEL_OTUS = 2
_NEXEL_OTU = 3
_NEXEL_TREES = 4
_NEXEL_TREE = 5
_NEXEL_NODE = 6
_NEXEL_EDGE = 7
_NEXEL_META = 8

_NEXEL_CODE_TO_STR = {
    _NEXEL_TOP_LEVEL: 'top-level',
    _NEXEL_NEXML: 'nexml element',
    _NEXEL_OTUS: 'otus group',
    _NEXEL_OTU: 'otu',
    _NEXEL_TREES: 'trees group',
    _NEXEL_TREE: 'tree',
    _NEXEL_NODE: 'node',
    _NEXEL_EDGE: 'edge',
    _NEXEL_META: 'meta',
}

class LazyAddress(object):
    @staticmethod
    def _address_code_to_str(code):
        return _NEXEL_CODE_TO_STR[code]
    def __init__(self, code, obj=None, obj_id=None, par_addr=None):
        self.code = code
        self.ref = obj
        if obj_id is None:
            self.obj_id = getattr(obj, '@id', None)
        else:
            self.obj_id = obj_id
        self.par_addr = par_addr
        self._path = None
    def write_path_suffix_str(self, out):
        p = self.path
        out.write(' in ')
        out.write(p)
    def get_path(self):
        if self._path is None:
            ts = LazyAddress._address_code_to_str(self.code)
            if self.obj_id is None:
                self._path = ts
            else:
                self._path = '{t} (id="{i}")'.format(t=ts, i=self.obj_id)
        return self._path
    path = property(get_path)
_EMPTY_TUPLE = tuple()

class NexsonValidationAdaptor(object):
    '''An object created during NexSON validation.
    It holds onto the nexson object that it was instantiated for.
    When add_or_replace_annotation is called, it will annotate the 
    nexson object, and when get_nexson_str is called it will
    serialize it.

    This class is useful merely because it allows the validation log
        and annotations to be relatively light weight, and yet easy 
        to efficiently add back to the orignal NexSON object.
    '''
    def __init__(self, obj, logger):
        self._raw = obj
        self._nexml = None
        self._pyid_to_nexson_add = {}
        self._logger = logger
        uk = None
        for k in obj.keys():
            if k not in ['nexml', 'nex:nexml']:
                if uk is None:
                    uk = []
                uk.append(k)
        if uk:
            uk.sort()
            self._warn_event(_NEXEL_TOP_LEVEL,
                             obj=obj,
                             err_type=gen_UnrecognizedKeyWarning,
                             anc=_EMPTY_TUPLE,
                             key_list=tuple(uk))
        self._nexml = None
        try:
            self._nexml = get_nexml_el(obj)
            assert(isinstance(self._nexml, dict))
        except:
            self._error_event(_NEXEL_TOP_LEVEL, 
                              obj=obj,
                              err_type=MissingMandatoryKeyWarning,
                              anc=_EMPTY_TUPLE,
                              key_list=('nexml',))
            return ## EARLY EXIT!!
        self._nexson_version = detect_nexson_version(obj)
        if _is_by_id_hbf(self._nexson_version):
            self.__class__ = ByIdHBFValidationAdaptor
        elif _is_badgerfish_version(self._nexson_version):
            self.__class__ = BadgerFishValidationAdaptor
        elif _is_direct_hbf(self._nexson_version):
            self.__class__ = DirectHBFValidationAdaptor
        else:
            assert(False) # unrecognized nexson variant
        self._validate_nexml_obj(self._nexml, anc=obj)

    def _event_address(self, obj_code, obj, anc, anc_offset=0):
        pyid = id(obj)
        addr = self._pyid_to_nexson_add.get(pyid)
        if addr is None:
            if len(anc) > anc_offset:
                p_ind = -1 -anc_offset
                p = anc[p_ind]
                par_addr = self._event_address(_get_par_obj_code(obj_code),
                                               p,
                                               anc, 
                                               1 + anc_offset)
            else:
                par_addr = None
            addr = LazyAddress(obj_code, obj=obj, par_addr=par_addr)
            self._pyid_to_nexson_add[pyid] = addr
        return addr, pyid
    def _warn_event(self, obj_code, obj, err_type, anc, *valist, **kwargs):
        c = factory2code[err_type]
        if not self._logger.is_logging_type(c):
            return
        address, pyid = self._event_address(obj_code, obj, anc)
        err_type(address, pyid, self._logger, SeverityCodes.WARNING *valist, **kwargs)
    def _error_event(self, obj_code, obj, err_type, anc, *valist, **kwargs):
        c = factory2code[err_type]
        if not self._logger.is_logging_type(c):
            return
        address, pyid = self._event_address(obj_code, obj, anc)
        err_type(address, pyid, self._logger, SeverityCodes.ERROR *valist, **kwargs)
    def _validate_nexml_obj(self, nex_obj, anc):
        pass

    def add_or_replace_annotation(self, annotation):
        '''Takes an `annotation` dictionary which is 
        expected to have a string as the value of annotation['author']['name']
        This function will remove all annotations from obj that:
            1. have the same author/name, and
            2. have no messages that are flagged as messages to be preserved (values for 'preserve' that evaluate to true)
        '''
        return # TODO!
        script_name = annotation['author']['name']
        n = obj['nexml']
        former_meta = n.setdefault('meta', [])
        if not isinstance(former_meta, list):
            former_meta = [former_meta]
            n['meta'] = former_meta
        else:
            indices_to_pop = []
            for annotation_ind, el in enumerate(former_meta):
                try:
                    if (el.get('$') == annotation_label) and (el.get('author',{}).get('name') == script_name):
                        m_list = el.get('messages', [])
                        to_retain = []
                        for m in m_list:
                            if m.get('preserve'):
                                to_retain.append(m)
                        if len(to_retain) == 0:
                            indices_to_pop.append(annotation_ind)
                        elif len(to_retain) < len(m_list):
                            el['messages'] = to_retain
                            el['dateModified'] = datetime.datetime.utcnow().isoformat()
                except:
                    # different annotation structures could yield IndexErrors or other exceptions.
                    # these are not the annotations that you are looking for....
                    pass

            if len(indices_to_pop) > 0:
                # walk backwards so pops won't change the meaning of stored indices
                for annotation_ind in indices_to_pop[-1::-1]:
                    former_meta.pop(annotation_ind)
        former_meta.append(annotation)
    def get_nexson_str(self):
        return json.dumps(self._raw, sort_keys=True, indent=0)


class ByIdHBFValidationAdaptor(NexsonValidationAdaptor):
    pass
class DirectHBFValidationAdaptor(NexsonValidationAdaptor):
    pass
class BadgerFishValidationAdaptor(NexsonValidationAdaptor):
    pass