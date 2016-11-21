# !/usr/bin/env python
"""

"""
from peyotl.utility.str_util import string_types_tuple
_string_types = string_types_tuple()
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


class SimpleCuratorSchema(object):
    required_elements = {}
    optional_elements = {
        'login': _string_types,  # not present in initial request
        'name': _string_types,
        'email': _string_types,  # provided by some agents
    }
    allowed_elements = frozenset(optional_elements.keys())

