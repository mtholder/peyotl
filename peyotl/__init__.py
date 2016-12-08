#!/usr/bin/env python
"""
Small library for conducting operations over the
entire set of NexSON files in one or more phylesystem
repositories.
"""
# Some imports to help our py2 code behave like py3
from __future__ import absolute_import, print_function, division

__version__ = '0.1.4dev'  # sync with setup.py

from peyotl.utility import (doi2url,
                            expand_path, expand_abspath,
                            get_config_setting, get_config_object, get_logger,
                            HTMLParser,
                            SafeConfigParser, slugify, string_types_tuple, StringIO, strip_tags)
from peyotl.utility.input_output import pretty_dict_str
from peyotl.collections_store import (collection_to_included_trees, concatenate_collections,
                                      tree_is_in_collection)
from peyotl.nexson_syntax import (add_cc0_waiver,
                                  can_convert_nexson_forms, convert_nexson_format,
                                  detect_nexson_version,
                                  extract_tree_nexson,
                                  get_ot_study_info_from_nexml,
                                  is_by_id_hbf,
                                  read_as_json,
                                  write_as_json, write_obj_as_nexml)
from peyotl.phylesystem import Phylesystem
from peyotl.utility.str_util import UNICODE, is_str_type
from peyotl.phylo.entities import OTULabelStyleEnum
from peyotl.git_storage import (get_phylesystem_repo_parent, get_doc_store_repo_parent)


def gen_otu_dict(nex_obj, nexson_version=None):
    """Takes a NexSON object and returns a dict of
    otu_id -> otu_obj
    """
    if nexson_version is None:
        nexson_version = detect_nexson_version(nex_obj)
    if is_by_id_hbf(nexson_version):
        otus = nex_obj['nexml']['otusById']
        if len(otus) > 1:
            d = {}
            for v in otus.values():
                d.update(v['otuById'])
            return d
        else:
            return otus.values()[0]['otuById']
    o_dict = {}
    for ob in nex_obj.get('otus', []):
        for o in ob.get('otu', []):
            oid = o['@id']
            o_dict[oid] = o
    return o_dict


def iter_tree(nex_obj):
    """Generator over each tree object in the NexSON object."""
    for tb in nex_obj.get('trees', []):
        for tree in tb.get('tree', []):
            yield tree


def iter_node(tree):
    """Generator over each node object in the tree object."""
    for nd in tree.get('nodeById', {}).items():
        yield nd


__all__ = ['utility',
           'api',
           'nexson_proxy',
           'nexson_syntax',
           'nexson_validation',
           'ott',
           'phylesystem',
           'string',
           'sugar',
           'test',
           'utility',
           'external',
           'manip',
           'struct_diff',
           'evaluate_tree',
           'validation',
           ]
from peyotl.phylesystem import NexsonDocSchema
from peyotl.git_storage.git_workflow import GitWorkflowError
from peyotl.external import import_nexson_from_treebase, import_nexson_from_crossref_metadata

# It is important to keep these imports last an in this order...
from peyotl.git_versioned_doc_store_collection import create_doc_store_wrapper
from peyotl.api import OTI
