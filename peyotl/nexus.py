#!/usr/bin/env python
# Some imports to help our py2 code behave like py3
from __future__ import absolute_import, print_function, division
from .utility import get_utf_8_string_io_writer, flush_utf_8_writer, get_utf_8_value
import re

NEXUS_NEEDING_QUOTING = re.compile(r'(\s|[-()\[\]{}/\\,;:=*"`+<>])')


def write_nexus_format(quoted_leaf_labels, tree_name_newick_list):
    if not tree_name_newick_list:
        return ''
    f, wrapper = get_utf_8_string_io_writer()
    wrapper.write('''#NEXUS
BEGIN TAXA;
    Dimensions NTax = {s};
    TaxLabels {q} ;
END;
BEGIN TREES;
'''.format(s=len(quoted_leaf_labels), q=' '.join(quoted_leaf_labels)))
    for name, newick in tree_name_newick_list:
        wrapper.write('    Tree ')
        wrapper.write(name)
        wrapper.write(' = ')
        wrapper.write(newick)
    wrapper.write('\nEND;\n')
    flush_utf_8_writer(wrapper)
    return get_utf_8_value(f)
