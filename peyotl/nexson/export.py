#!/usr/bin/env python
# Some imports to help our py2 code behave like py3
from __future__ import absolute_import, print_function, division

from ..newick import NEWICK_NEEDING_QUOTING, quote_newick_name
from ..nexus import NEXUS_NEEDING_QUOTING, write_nexus_format
from ..utility import (get_utf_8_string_io_writer, flush_utf_8_writer, get_utf_8_value)

_EMPTY_TUPLE = tuple


def _write_newick_leaf_label(out,
                             node,
                             otu_group,
                             labeller,
                             leaf_labels,
                             unlabeled_counter,
                             needs_quotes_pattern):
    """
    `labeller` is a string (a key in the otu object) or a callable that takes two arguments: the node, and the otu
    If `leaf_labels` is not None, it shoulr be a (list, dict) pair which will be filled. The list will
        hold the order encountered,
        and the dict will map name to index in the list
    """
    otu_id = node['@otu']
    otu = otu_group[otu_id]
    label = quote_newick_name(labeller(node, otu, unlabeled_counter), needs_quotes_pattern)
    if leaf_labels is not None:
        if label not in leaf_labels[1]:
            leaf_labels[1][label] = len(leaf_labels[0])
            leaf_labels[0].append(label)
    out.write(label)
    return unlabeled_counter


def _write_newick_internal_label(out, node, otu_group, labeller, needs_quotes_pattern):
    """`labeller` is a callable that takes two arguments: the node, and the otu (which may be
        None for an internal node)
    If `leaf_labels` is not None, it shoulr be a (list, dict) pair which will be filled. The list will
        hold the order encountered,
        and the dict will map name to index in the list
    """
    otu_id = node.get('@otu')
    otu = None if otu_id is None else otu_group[otu_id]
    label = labeller(node, otu, None)
    if label is not None:
        label = quote_newick_name(label, needs_quotes_pattern)
        out.write(label)


def _write_newick_edge_len(out, edge):
    if edge is None:
        return
    e_len = edge.get('@length')
    if e_len is not None:
        out.write(':{e}'.format(e=e_len))


def nexson_frag_write_newick(out,
                             edges,
                             nodes,
                             otu_group,
                             labeller,
                             leaf_labels,
                             root_id,
                             needs_quotes_pattern=NEWICK_NEEDING_QUOTING,
                             ingroup_id=None,
                             bracket_ingroup=False,
                             with_edge_lengths=True):
    """`labeller` is a callable that takes two arguments: the node, and the otu (which may be
            None for an internal node)
    If `leaf_labels` is not None, it shoulr be a (list, dict) pair which will be filled. The list will
        hold the order encountered,
        and the dict will map name to index in the list
    """
    unlabeled_counter = 0
    curr_node_id = root_id
    assert curr_node_id
    curr_edge = None
    curr_sib_list = []
    curr_stack = []
    going_tipward = True
    while True:
        if going_tipward:
            outgoing_edges = edges.get(curr_node_id)
            if outgoing_edges is None:
                curr_node = nodes[curr_node_id]
                assert curr_node_id is not None
                assert curr_node_id is not None
                unlabeled_counter = _write_newick_leaf_label(out,
                                                             curr_node,
                                                             otu_group,
                                                             labeller,
                                                             leaf_labels,
                                                             unlabeled_counter,
                                                             needs_quotes_pattern)
                if with_edge_lengths:
                    _write_newick_edge_len(out, curr_edge)
                going_tipward = False
            else:
                te = [(i, e) for i, e in outgoing_edges.items()]
                te.sort()  # produce a consistent rotation... Necessary?
                if bracket_ingroup and (ingroup_id == curr_node_id):
                    out.write('[pre-ingroup-marker]')
                out.write('(')
                next_p = te.pop(0)
                curr_stack.append((curr_edge, curr_node_id, curr_sib_list))
                curr_edge, curr_sib_list = next_p[1], te
                curr_node_id = curr_edge['@target']
        if not going_tipward:
            next_up_edge_id = None
            while True:
                if curr_sib_list:
                    out.write(',')
                    next_up_edge_id, next_up_edge = curr_sib_list.pop(0)
                    break
                if curr_stack:
                    curr_edge, curr_node_id, curr_sib_list = curr_stack.pop(-1)
                    curr_node = nodes[curr_node_id]
                    out.write(')')
                    _write_newick_internal_label(out,
                                                 curr_node,
                                                 otu_group,
                                                 labeller,
                                                 needs_quotes_pattern)
                    if with_edge_lengths:
                        _write_newick_edge_len(out, curr_edge)
                    if bracket_ingroup and (ingroup_id == curr_node_id):
                        out.write('[post-ingroup-marker]')
                else:
                    break
            if next_up_edge_id is None:
                break
            # noinspection PyUnboundLocalVariable
            curr_edge = next_up_edge
            curr_node_id = curr_edge['@target']
            going_tipward = True
    out.write(';')


def convert_tree_to_newick(tree,
                           otu_group,
                           labeller,
                           leaf_labels,
                           needs_quotes_pattern=NEWICK_NEEDING_QUOTING,
                           subtree_id=None,
                           bracket_ingroup=False):
    """`labeller` is a callable that takes two arguments:
        the node, and the otu (which may be None for an internal node)
    If `leaf_labels` is not None, it shoulr be a (list, dict) pair which will be filled. The list will
        hold the order encountered,
        and the dict will map name to index in the list
    """
    ingroup_node_id = tree.get('^ot:inGroupClade')
    if subtree_id:
        if subtree_id == 'ingroup':
            root_id = ingroup_node_id
            ingroup_node_id = None  # turns of the comment pre-ingroup-marker
        else:
            root_id = subtree_id
    else:
        root_id = tree['^ot:rootNodeId']
    edges = tree['edgeBySourceId']
    if root_id not in edges:
        return None
    nodes = tree['nodeById']
    sio, out = get_utf_8_string_io_writer()
    nexson_frag_write_newick(out,
                             edges,
                             nodes,
                             otu_group,
                             labeller,
                             leaf_labels,
                             root_id,
                             needs_quotes_pattern=needs_quotes_pattern,
                             ingroup_id=ingroup_node_id,
                             bracket_ingroup=bracket_ingroup)
    flush_utf_8_writer(out)
    return get_utf_8_value(sio)


def convert_tree(tree_id, tree, otu_group, schema, subtree_id=None):
    if schema.format_str == 'nexus':
        leaf_labels = ([], {})
        needs_quotes_pattern = NEXUS_NEEDING_QUOTING
    else:
        leaf_labels = None
        needs_quotes_pattern = NEWICK_NEEDING_QUOTING
        assert schema.format_str == 'newick'
    newick = convert_tree_to_newick(tree,
                                    otu_group,
                                    schema.otu_labeller,
                                    leaf_labels,
                                    needs_quotes_pattern,
                                    subtree_id=subtree_id,
                                    bracket_ingroup=schema.bracket_ingroup)
    if schema.format_str == 'nexus':
        tl = [(quote_newick_name(tree_id, needs_quotes_pattern), newick)]
        return write_nexus_format(leaf_labels[0], tl)
    return newick


def convert_trees(tid_tree_otus_list, schema, subtree_id=None):
    if schema.format_str == 'nexus':
        leaf_labels = ([], {})
        needs_quotes_pattern = NEXUS_NEEDING_QUOTING
        conv_tree_list = []
        for tree_id, tree, otu_group in tid_tree_otus_list:
            newick = convert_tree_to_newick(tree,
                                            otu_group,
                                            schema.otu_labeller,
                                            leaf_labels,
                                            needs_quotes_pattern,
                                            subtree_id=subtree_id,
                                            bracket_ingroup=schema.bracket_ingroup)
            if newick:
                t = (quote_newick_name(tree_id, needs_quotes_pattern), newick)
                conv_tree_list.append(t)
        return write_nexus_format(leaf_labels[0], conv_tree_list)
    else:
        raise NotImplementedError('convert_tree for {}'.format(schema.format_str))


# noinspection PyProtectedMember
def write_obj_as_nexml(obj_dict,
                       file_obj,
                       addindent='',
                       newl='',
                       use_default_root_atts=True,
                       otu_label='ot:originalLabel'):
    from .helper import (ConversionConfig, convert_nexson_format,
                         detect_nexson_version, _DIRECT_HONEY_BADGERFISH,
                         _nexson_directly_translatable_to_nexml,
                         Nexson2Nexml)
    nsv = detect_nexson_version(obj_dict)
    if not _nexson_directly_translatable_to_nexml(nsv):
        convert_nexson_format(obj_dict, _DIRECT_HONEY_BADGERFISH)
        nsv = _DIRECT_HONEY_BADGERFISH
    ccfg = ConversionConfig('nexml',
                            input_format=nsv,
                            use_default_root_atts=use_default_root_atts,
                            otu_label=otu_label)
    converter = Nexson2Nexml(ccfg)
    doc = converter.convert(obj_dict)
    doc.writexml(file_obj, addindent=addindent, newl=newl, encoding='utf-8')


def convert_to_nexml(obj_dict, addindent='', newl='', use_default_root_atts=True,
                     otu_label='ot:originalLabel'):
    f, wrapper = get_utf_8_string_io_writer()
    write_obj_as_nexml(obj_dict,
                       file_obj=wrapper,
                       addindent=addindent,
                       newl=newl,
                       use_default_root_atts=use_default_root_atts,
                       otu_label=otu_label)
    flush_utf_8_writer(wrapper)
    return f.getvalue()
