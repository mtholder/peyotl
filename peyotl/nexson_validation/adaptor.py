#!/usr/bin/env python
'''NexsonValidationAdaptor class.
'''
from peyotl.nexson_validation._validation_base import NexsonValidationAdaptor
from peyotl.nexson_validation._badgerfish_validation import BadgerFishValidationAdaptor
from peyotl.nexson_validation._by_id_validation import ByIdHBFValidationAdaptor
from peyotl.nexson_syntax import detect_nexson_version
from peyotl.nexson_syntax.helper import _add_value_to_dict_bf, \
                                        _is_badgerfish_version, \
                                        _is_by_id_hbf, \
                                        _is_direct_hbf
from peyotl.utility import get_logger
_LOG = get_logger(__name__)


class DirectHBFValidationAdaptor(BadgerFishValidationAdaptor):
    def __init__(self, obj, logger):
        NexsonValidationAdaptor.__init__(self, obj, logger)

def create_validation_adaptor(obj, logger):
    try:
        nexson_version = detect_nexson_version(obj)
    except:
        return BadgerFishValidationAdaptor(obj, logger)
    if _is_by_id_hbf(nexson_version):
        return ByIdHBFValidationAdaptor(obj, logger)
    elif _is_badgerfish_version(nexson_version):
        return BadgerFishValidationAdaptor(obj, logger)
    elif _is_direct_hbf(nexson_version):
        return DirectHBFValidationAdaptor(obj, logger)
    raise NotImplementedError('nexml2json version {v}'.format(v=nexson_version))
