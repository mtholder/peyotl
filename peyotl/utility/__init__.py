#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Simple utility functions that do not depend on any other part of
peyotl.
"""
from peyotl.utility.input_output import (download, expand_path, expand_abspath, parse_study_tree_list,
                                         write_to_filepath)
from peyotl.utility.str_util import is_str_type, string_types_tuple, slugify
import peyotl.utility.get_logger
from peyotl.utility.get_logger import get_logger
from peyotl.utility.get_config import (ConfigWrapper, get_config_setting, get_config_object, read_config,
                                       get_raw_default_config_and_read_file_list)
import time
import os

__all__ = ['input_output', 'simple_file_lock', 'str_util', 'get_logger', 'dict_wrapper', 'tokenizer', 'get_config']


def any_early_exit(iterable, predicate):
    """Tests each element in iterable by calling predicate(element). Returns True on first True, or False."""
    for i in iterable:
        if predicate(i):
            return True
    return False


def pretty_timestamp(t=None, style=0):
    if t is None:
        t = time.localtime()
    if style == 0:
        return time.strftime("%Y-%m-%d", t)
    return time.strftime("%Y%m%d%H%M%S", t)


def doi2url(v):
    if v.startswith('http'):
        return v
    if v.startswith('doi:'):
        if v.startswith('doi: '):
            v = v[5:]  # trim 'doi: '
        else:
            v = v[4:]  # trim 'doi:'
    if v.startswith('10.'):  # it's a DOI!
        return 'http://dx.doi.org/' + v
    # convert anything else to URL and hope for the best
    return 'http://' + v


def get_unique_filepath(stem):
    """NOT thread-safe!
    return stems or stem# where # is the smallest
    positive integer for which the path does not exist.
    useful for temp dirs where the client code wants an
    obvious ordering.
    """
    fp = stem
    if os.path.exists(stem):
        n = 1
        fp = stem + str(n)
        while os.path.exists(fp):
            n += 1
            fp = stem + str(n)
    return fp


def propinquity_fn_to_study_tree(inp_fn, strip_extension=True):
    """This should only be called by propinquity - other code should be treating theses
    filenames (and the keys that are based on them) as opaque strings.

    Takes a filename (or key if strip_extension is False), returns (study_id, tree_id)

    propinquity provides a map to look up the study ID and tree ID (and git SHA)
    from these strings.
    """
    if strip_extension:
        study_tree = '.'.join(inp_fn.split('.')[:-1])  # strip extension
    else:
        study_tree = inp_fn
    x = study_tree.split('@')
    if len(x) != 2:
        msg = 'Currently we are expecting studyID@treeID.<file extension> format. ' \
              'Expected exactly 1 @ in the filename. Got "{}"'
        msg = msg.format(study_tree)
        raise ValueError(msg)
    return x

def validate_dict_keys(obj, schema, errors, name):
    """Takes a dict `obj` and a simple `schema` that is expected to have:
    `schema.required_elements` and `schema.optional_elements` dicts
    mapping names of properties of `obj` to types that can be second args
    to isinstance.
    `schema.allowed_elements` should be a set of allowed properties

    error strings are appended to the list `errors`, and `name` is used
    in error strings to describe `obj`
    """
    uk = [k for k in obj.keys() if k not in schema.allowed_elements]
    if uk:
        uk.sort()
        msg = 'Found these unexpected properties in a {n} object: "{k}"'
        msg = msg.format(n=name, k='", "'.join(uk))
        errors.append(msg)
    # test for existence and types of all required elements
    for el_key, el_type in schema.required_elements.items():
        test_el = obj.get(el_key)
        if test_el is None:
            errors.append("Property '{p}' not found!".format(p=el_key))
        elif not isinstance(test_el, el_type):
            errors.append("Property '{p}' should be one of these: {t}".format(p=el_key, t=el_type))
    # test for types of optional elements
    for el_key, el_type in schema.optional_elements.items():
        test_el = obj.get(el_key)
        if (test_el is not None) and not isinstance(test_el, el_type):
            errors.append("Property '{p}' should be one of these: {t}".format(p=el_key, t=el_type))

