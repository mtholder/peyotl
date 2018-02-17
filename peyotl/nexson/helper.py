#!/usr/bin/env python
# -*- coding: utf-8 -*-
from ..phylo_syntax import PhyloSyntax, nexson_syntaxes
from ..utility import logger, is_int_type, UNICODE
import xml
import re

_DIRECT_HONEY_BADGERFISH = PhyloSyntax.NEXSON_1_0
_BY_ID_HONEY_BADGERFISH = PhyloSyntax.NEXSON_1_2
_BADGER_FISH_NEXSON_VERSION = PhyloSyntax.NEXSON_0
_LITERAL_META_PAT = re.compile(r'.*[:]?LiteralMeta$')
_RESOURCE_META_PAT = re.compile(r'.*[:]?ResourceMeta$')


class NexsonError(Exception):
    def __init__(self, v):
        self.value = v

    def __str__(self):
        return repr(self.value)


def _is_badgerfish_version(x):
    return x == PhyloSyntax.NEXSON_0


def _is_direct_hbf(x):
    return x == PhyloSyntax.NEXSON_1_0


def _is_by_id_hbf(x):
    return x == PhyloSyntax.NEXSON_1_2


def _is_supported_nexson_vers(x):
    return x in nexson_syntaxes


def _index_list_of_values(d, k):
    """Returns d[k] or [d[k]] if the value is not a list"""
    v = d[k]
    if isinstance(v, list):
        return v
    return [v]


def cull_nonmatching_trees(nexson, tree_id, src_syntax=None):
    """Modifies `nexson` and returns it in version 1.2.1
    with any tree that does not match the ID removed.

    Note that this does not search through the NexSON for
    every node, edge, tree that was deleted. So the resulting
    NexSON may have broken references !
    """
    if src_syntax is None:
        src_syntax = detect_nexson_version(nexson)
    if not _is_by_id_hbf(src_syntax):
        nexson = convert_nexson_format(nexson, _BY_ID_HONEY_BADGERFISH)

    nexml_el = get_nexml_el(nexson)
    tree_groups = nexml_el['treesById']
    tree_groups_to_del = []
    for tgi, tree_group in tree_groups.items():
        tbi = tree_group['treeById']
        if tree_id in tbi:
            trees_to_del = [i for i in tbi.keys() if i != tree_id]
            for tid in trees_to_del:
                tree_group['^ot:treeElementOrder'].remove(tid)
                del tbi[tid]
        else:
            tree_groups_to_del.append(tgi)
    for tgid in tree_groups_to_del:
        nexml_el['^ot:treesElementOrder'].remove(tgid)
        del tree_groups[tgid]
    return nexson


def nexml_el_of_by_id(nexson, src_syntax=None):
    if src_syntax is None:
        src_syntax = detect_nexson_version(nexson)
    if not _is_by_id_hbf(src_syntax):
        nexson = convert_nexson_format(nexson, _BY_ID_HONEY_BADGERFISH)
    return get_nexml_el(nexson)


def extract_otus_nexson(nexson, otus_id, src_syntax=None):
    nexml_el = nexml_el_of_by_id(nexson, src_syntax)
    o = nexml_el['otusById']
    if otus_id is None:
        return o
    n = o.get(otus_id)
    if n is None:
        return None
    return {otus_id: n}


def extract_otu_nexson(nexson, otu_id, src_syntax=None):
    nexml_el = nexml_el_of_by_id(nexson, src_syntax)
    o = nexml_el['otusById']
    if otu_id is None:
        r = {}
        for g in o.values():
            r.update(g.get('otuById', {}))
        return r
    else:
        for g in o.values():
            go = g['otuById']
            if otu_id in go:
                return {otu_id: go[otu_id]}
    return None


def extract_tree_nexson(nexson, tree_id, src_syntax=None):
    """Returns a list of (id, tree, otus_group) tuples for the
    specified tree_id (all trees if tree_id is None)
    """
    if src_syntax is None:
        src_syntax = detect_nexson_version(nexson)
    if not _is_by_id_hbf(src_syntax):
        nexson = convert_nexson_format(nexson, _BY_ID_HONEY_BADGERFISH)

    nexml_el = get_nexml_el(nexson)
    tree_groups = nexml_el['treesById']
    tree_obj_otus_group_list = []
    for tree_group in tree_groups.values():
        if tree_id:
            tree_list = [(tree_id, tree_group['treeById'].get(tree_id))]
        else:
            tree_list = tree_group['treeById'].items()
        for tid, tree in tree_list:
            if tree is not None:
                otu_groups = nexml_el['otusById']
                ogi = tree_group['@otus']
                otu_group = otu_groups[ogi]['otuById']
                tree_obj_otus_group_list.append((tid, tree, otu_group))
                if tree_id is not None:
                    return tree_obj_otus_group_list
    return tree_obj_otus_group_list


def get_nexml_el(blob):
    v = blob.get('nexml')
    if v is not None:
        return v
    return blob['nex:nexml']


def detect_nexson_version(blob):
    """Returns the nexml2json attribute or the default code for badgerfish"""
    n = get_nexml_el(blob)
    assert isinstance(n, dict)
    return n.get('@nexml2json', _BADGER_FISH_NEXSON_VERSION)


def _recursive_sort_meta(blob, k):
    if isinstance(blob, list):
        for i in blob:
            if isinstance(i, list) or isinstance(i, dict):
                _recursive_sort_meta(i, k)
    else:
        for inner_k, v in blob.items():
            if inner_k == 'meta' and isinstance(v, list):
                sl = []
                incd = {}
                for el in v:
                    sk = el.get('@property') or el.get('@rel') or ''
                    count = incd.setdefault(sk, 0)
                    incd[sk] = 1 + count
                    sl.append((sk, count, el))
                sl.sort()
                del v[:]  # clear out the value in place
                v.extend([i[2] for i in sl])  # replace it with the item from the sorted list
            if isinstance(v, list) or isinstance(v, dict):
                _recursive_sort_meta(v, inner_k)


def _get_index_list_of_values(d, k, def_value=None):
    """Like _index_list_of_values, but uses get to access and
    returns an empty list if the key is absent.
    Returns d[k] or [d[k]] if the value is not a list"""
    v = d.get(k, def_value)
    if v is None:
        return []
    if isinstance(v, list):
        return v
    return [v]


def _inplace_sort_by_id(unsorted_list):
    """Takes a list of dicts each of which has an '@id' key,
    sorts the elements in the list by the value of the @id key.
    Assumes that @id is unique or the dicts have a meaningul < operator
    """
    if not isinstance(unsorted_list, list):
        return
    sorted_list = [(i.get('@id'), i) for i in unsorted_list]
    sorted_list.sort()
    del unsorted_list[:]
    unsorted_list.extend([i[1] for i in sorted_list])


def sort_meta_elements(blob, syntax=None):
    """For v0.0 (which has meta values in a list), this
    function recursively walks through the object
    and sorts each meta by @property or @rel values.
    """
    if syntax is None:
        syntax = detect_nexson_version(blob)
    if syntax == _BADGER_FISH_NEXSON_VERSION:
        _recursive_sort_meta(blob, '')
    return blob


def sort_arbitrarily_ordered_nexson(blob, syntax=None):
    """Primarily used for testing (getting nice diffs). Calls
    sort_meta_elements and then sorts otu, node and edge list by id
    """
    # otu, node and edge elements have no necessary orger in v0.0 or v1.0
    if syntax is None:
        syntax = detect_nexson_version(blob)
    if syntax == _BY_ID_HONEY_BADGERFISH:
        return blob
    sort_meta_elements(blob, syntax)
    nex = get_nexml_el(blob)
    for ob in _get_index_list_of_values(nex, 'otus'):
        _inplace_sort_by_id(ob.get('otu', []))
    for tb in _get_index_list_of_values(nex, 'trees'):
        for tree in _get_index_list_of_values(tb, 'tree'):
            _inplace_sort_by_id(tree.get('node', []))
            _inplace_sort_by_id(tree.get('edge', []))
    return blob


class ConversionConfig(object):
    def __init__(self, output_format, **kwargs):
        self._keys = ['output_format']
        self.output_format = output_format
        for k, v in kwargs.items():
            self.__dict__[k] = v
            self._keys.append(k)

    def items(self):
        for k in self._keys:
            yield (k, getattr(self, k))

    def keys(self):
        return list(self._keys)

    def get(self, k, default):
        return getattr(self, k, default)


def convert_nexson_format(src,
                          dest_syntax,
                          src_syntax=None,
                          remove_old_structs=True,
                          pristine_if_invalid=False,
                          sort_arbitrary=False):
    """Take a dict form of NexSON and converts its datastructures to
    those needed to serialize as dest_syntax.
    If src_syntax is not specified, it will be inferred.
    If `remove_old_structs` is False and different honeybadgerfish varieties
        are selected, the `src` will be 'fat" containing both types
        of lookup structures.
    If pristine_if_invalid is False, then the object may be corrupted if it
        is an invalid nexson struct. Setting this to False can result in
        faster translation, but if an exception is raised the object may
        be polluted with partially constructed fields for the dest_syntax.
    """
    if src_syntax is None:
        src_syntax = detect_nexson_version(src)
    if src_syntax == dest_syntax:
        if sort_arbitrary:
            sort_arbitrarily_ordered_nexson(src, src_syntax)
        return src
    two2zero = _is_by_id_hbf(dest_syntax) and _is_badgerfish_version(src_syntax)
    zero2two = _is_by_id_hbf(src_syntax) and _is_badgerfish_version(dest_syntax)
    if two2zero or zero2two:
        # go from 0.0 -> 1.0 then the 1.0->1.2 should succeed without nexml...
        src = convert_nexson_format(src,
                                    _DIRECT_HONEY_BADGERFISH,
                                    src_syntax=src_syntax,
                                    remove_old_structs=remove_old_structs,
                                    pristine_if_invalid=pristine_if_invalid)
        src_syntax = _DIRECT_HONEY_BADGERFISH

    ccdict = {'output_format': dest_syntax,
              'input_format': src_syntax,
              'remove_old_structs': remove_old_structs,
              'pristine_if_invalid': pristine_if_invalid}
    ccfg = ConversionConfig(ccdict)
    if _is_badgerfish_version(src_syntax):
        from .badgerfish2direct_nexson import Badgerfish2DirectNexson
        converter = Badgerfish2DirectNexson(ccfg)
    elif _is_badgerfish_version(dest_syntax):
        assert _is_direct_hbf(src_syntax)
        from .direct2badgerfish_nexson import Direct2BadgerfishNexson
        converter = Direct2BadgerfishNexson(ccfg)
    elif _is_direct_hbf(src_syntax) and (dest_syntax == _BY_ID_HONEY_BADGERFISH):
        from .direct2optimal_nexson import Direct2OptimalNexson
        converter = Direct2OptimalNexson(ccfg)
    elif _is_direct_hbf(dest_syntax) and (src_syntax == _BY_ID_HONEY_BADGERFISH):
        from .optimal2direct_nexson import Optimal2DirectNexson
        converter = Optimal2DirectNexson(ccfg)
    else:
        raise NotImplementedError('Conversion from {i} to {o}'.format(i=src_syntax, o=dest_syntax))
    src = converter.convert(src)
    if sort_arbitrary:
        sort_arbitrarily_ordered_nexson(src)
    return src


class NexsonConverter(object):
    def __init__(self, conv_cfg):
        self._conv_cfg = conv_cfg
        for k, v in conv_cfg.items():
            self.__dict__[k] = v
        self.remove_old_structs = conv_cfg.get('remove_old_structs', True)
        self.pristine_if_invalid = conv_cfg.get('pristine_if_invalid', False)


def _add_redundant_about(obj):
    id_val = obj.get('@id')
    if id_val and ('@about' not in obj):
        obj['@about'] = ('#' + id_val)


def _contains_hbf_meta_keys(d):
    for k in d.keys():
        if k.startswith('^'):
            return True
    return False


def _python_instance_to_nexml_meta_datatype(v):
    """Returns 'xsd:string' or a more specific type for a <meta datatype="XYZ"...
    syntax using introspection.
    """
    if isinstance(v, bool):
        return 'xsd:boolean'
    if is_int_type(v):
        return 'xsd:int'
    if isinstance(v, float):
        return 'xsd:float'
    return 'xsd:string'


def _convert_hbf_meta_val_for_xml(key, val):
    """Convert to a BadgerFish-style dict for addition to a dict suitable for
    addition to XML tree or for v1.0 to v0.0 conversion."""
    if isinstance(val, list):
        return [_convert_hbf_meta_val_for_xml(key, i) for i in val]
    is_literal = True
    content = None
    if isinstance(val, dict):
        ret = val
        if '@href' in val:
            is_literal = False
        else:
            content = val.get('$')
            if isinstance(content, dict) and _contains_hbf_meta_keys(val):
                is_literal = False
    else:
        ret = {}
        content = val
    if is_literal:
        ret.setdefault('@xsi:type', 'nex:LiteralMeta')
        ret.setdefault('@property', key)
        if content is not None:
            ret.setdefault('@datatype', _python_instance_to_nexml_meta_datatype(content))
        if ret is not val:
            ret['$'] = content
    else:
        ret.setdefault('@xsi:type', 'nex:ResourceMeta')
        ret.setdefault('@rel', key)
    return ret


def _add_value_to_dict_bf(d, k, v):
    """Adds the `k`->`v` mapping to `d`, but if a previous element exists it changes
    the value of for the key to list.

    This is used in the BadgerFish mapping convention.

    This is a simple multi-dict that is only suitable when you know that you'll never
    store a list or `None` as a value in the dict.
    """
    prev = d.get(k)
    if prev is None:
        d[k] = v
    elif isinstance(prev, list):
        if isinstance(v, list):
            prev.extend(v)
        else:
            prev.append(v)
    else:
        if isinstance(v, list):
            x = [prev]
            x.extend(v)
            d[k] = x
        else:
            d[k] = [prev, v]


class NexmlTypeError(Exception):
    def __init__(self, m):
        self.msg = m

    def __str__(self):
        return self.msg


def _coerce_literal_val_to_primitive(datatype, str_val):
    _TYPE_ERROR_MSG_FORMAT = 'Expected meta property to have type {t}, but found "{v}"'
    if datatype == 'xsd:string':
        return str_val
    if datatype in frozenset(['xsd:int', 'xsd:integer', 'xsd:long']):
        try:
            return int(str_val)
        except:
            raise NexmlTypeError(_TYPE_ERROR_MSG_FORMAT.format(t=datatype, v=str_val))
    elif datatype == frozenset(['xsd:float', 'xsd:double']):
        try:
            return float(str_val)
        except:
            raise NexmlTypeError(_TYPE_ERROR_MSG_FORMAT.format(t=datatype, v=str_val))
    elif datatype == 'xsd:boolean':
        if str_val.lower() in frozenset(['1', 'true']):
            return True
        elif str_val.lower() in frozenset(['0', 'false']):
            return False
        else:
            raise NexmlTypeError(_TYPE_ERROR_MSG_FORMAT.format(t=datatype, v=str_val))
    else:
        logger(__name__).debug('unknown xsi:type "%s"', datatype)
        return None  # We'll fall through to here when we encounter types we do not recognize


def _cull_redundant_about(obj):
    """Removes the @about key from the `obj` dict if that value refers to the
    dict's '@id'
    """
    about_val = obj.get('@about')
    if about_val:
        id_val = obj.get('@id')
        if id_val and (('#' + id_val) == about_val):
            del obj['@about']


class Nexson2Nexml(NexsonConverter):
    """Conversion of the optimized (v 1.2) version of NexSON to
    the more direct (v 1.0) port of NeXML
    This is a dict-to-minidom-doc conversion. No serialization is included.
    """

    def __init__(self, conv_cfg):
        NexsonConverter.__init__(self, conv_cfg)
        self.input_format = conv_cfg.input_format
        self.use_default_root_atts = conv_cfg.get('use_default_root_atts', True)
        self.otu_label = conv_cfg.get('otu_label', 'ot:originalLabel')
        if self.otu_label.startswith('^'):
            self.otu_label = self.otu_label[1:]
        self._migrating_from_bf = _is_badgerfish_version(self.input_format)
        # TreeBase and phylografter trees often lack the tree xsi:type
        self._adding_tree_xsi_type = True
        # we have started using ot:ottTaxonName, ot:originalLabel or ot:ottId
        self._creating_otu_label = True

    def convert(self, blob):
        doc = xml.dom.minidom.Document()
        converted_root_el = False
        if 'nexml' in blob:
            converted_root_el = True
            blob['nex:nexml'] = blob['nexml']
            del blob['nexml']
        self._top_level_build_xml(doc, blob)
        if converted_root_el:
            blob['nexml'] = blob['nex:nexml']
            del blob['nex:nexml']

        return doc

    def _partition_keys_for_xml(self, o):
        """Breaks o into four content type by key syntax:
            attrib keys (start with '@'),
            text (value associated with the '$' or None),
            child element keys (all others)
            meta element
        """
        ak = {}
        tk = None
        ck = {}
        mc = {}
        # _LOG.debug('o = {o}'.format(o=o))
        for k, v in o.items():
            if k.startswith('@'):
                if k == '@xmlns':
                    if '$' in v:
                        ak['xmlns'] = v['$']
                    for nsk, nsv in v.items():
                        if nsk != '$':
                            ak['xmlns:' + nsk] = nsv
                else:
                    s = k[1:]
                    if isinstance(v, bool):
                        v = u'true' if v else u'false'
                    ak[s] = UNICODE(v)
            elif k == '$':
                tk = v
            elif k.startswith('^') and (not self._migrating_from_bf):
                s = k[1:]
                val = _convert_hbf_meta_val_for_xml(s, v)
                _add_value_to_dict_bf(mc, s, val)
            elif (k == u'meta') and self._migrating_from_bf:
                s, val = _convert_bf_meta_val_for_xml(v)
                _add_value_to_dict_bf(mc, s, val)
            else:
                ck[k] = v
        return ak, tk, ck, mc

    def _top_level_build_xml(self, doc, obj_dict):
        if self.use_default_root_atts:
            root_atts = {
                "xmlns:nex": "http://www.nexml.org/2009",
                "xmlns": "http://www.nexml.org/2009",
                "xmlns:xsd": "http://www.w3.org/2001/XMLSchema#",
                "xmlns:xsi": "http://www.w3.org/2001/XMLSchema-instance",
                "xmlns:ot": "http://purl.org/opentree/nexson",
            }
        else:
            root_atts = {}
        # extra = {
        #     "xmlns:dc": "http://purl.org/dc/elements/1.1/",
        #     "xmlns:dcterms": "http://purl.org/dc/terms/",
        #     "xmlns:prism": "http://prismstandard.org/namespaces/1.2/basic/",
        #     "xmlns:rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        #     "xmlns:rdfs": "http://www.w3.org/2000/01/rdf-schema#",
        #     "xmlns:skos": "http://www.w3.org/2004/02/skos/core#",
        #     "xmlns:tb": "http://purl.org/phylo/treebase/2.0/terms#",
        # }
        base_keys = list(obj_dict.keys())
        assert len(base_keys) == 1
        root_name = base_keys[0]
        root_obj = obj_dict[root_name]
        atts, data, children, meta_children = self._partition_keys_for_xml(root_obj)
        if 'generator' not in atts:
            atts['generator'] = 'org.opentreeoflife.api.nexsonvalidator.nexson_nexml'
        if 'version' not in atts:
            atts['version'] = '0.9'
        if root_atts:
            for k, v in root_atts.items():
                atts[k] = v
        if ('id' in atts) and ('about' not in atts):
            atts['about'] = '#' + atts['id']
        if 'nexml2json' in atts:
            del atts['nexml2json']
        r = _create_sub_el(doc, doc, root_name, atts, data)
        self._add_meta_dict_to_xml(doc, r, meta_children)
        nexml_key_order = (('meta', None),
                           ('otus', (('meta', None),
                                     ('otu', None)
                                     )
                            ),
                           ('characters', (('meta', None),
                                           ('format', (('meta', None),
                                                       ('states', (('state', None),
                                                                   ('uncertain_state_set', None),
                                                                   )
                                                        ),
                                                       ('char', None)
                                                       ),
                                            ),
                                           ('matrix', (('meta', None),
                                                       ('row', None),
                                                       )
                                            ),
                                           ),
                            ),
                           ('trees', (('meta', None),
                                      ('tree', (('meta', None),
                                                ('node', None),
                                                ('edge', None)
                                                )
                                       )
                                      )
                            )
                           )
        self._add_dict_of_subtree_to_xml_doc(doc, r, children, nexml_key_order)

    def _add_subtree_list_to_xml_doc(self, doc, par, ch_list, key, key_order):
        for child in ch_list:
            if isinstance(child, dict):
                self._add_subtree_to_xml_doc(doc, par, child, key, key_order)
            else:
                ca = {}
                cc = {}
                mc = {}
                if isinstance(child, list) or isinstance(child, tuple) or isinstance(child, set):
                    for sc in child:
                        if isinstance(sc, dict):
                            self._add_subtree_to_xml_doc(doc, par, sc, key, key_order)
                        else:
                            cd = sc
                            cel = _create_sub_el(doc, par, key, ca, cd)
                            self._add_meta_dict_to_xml(doc, cel, mc)
                            self._add_dict_of_subtree_to_xml_doc(doc, cel, cc, key_order=None)
                else:
                    cd = child
                    cel = _create_sub_el(doc, par, key, ca, cd)
                    self._add_meta_dict_to_xml(doc, cel, mc)
                    self._add_dict_of_subtree_to_xml_doc(doc, cel, cc, key_order=None)

    def _add_dict_of_subtree_to_xml_doc(self,
                                        doc,
                                        parent,
                                        children_dict,
                                        key_order=None):
        written = set()
        if key_order:
            for t in key_order:
                k, nko = t
                assert nko is None or isinstance(nko, tuple)
                if k in children_dict:
                    chl = _index_list_of_values(children_dict, k)
                    written.add(k)
                    self._add_subtree_list_to_xml_doc(doc, parent, chl, k, nko)
        ksl = list(children_dict.keys())
        ksl.sort()
        for k in ksl:
            chl = _index_list_of_values(children_dict, k)
            if k not in written:
                self._add_subtree_list_to_xml_doc(doc, parent, chl, k, None)

    def _add_subtree_to_xml_doc(self,
                                doc,
                                parent,
                                subtree,
                                key,
                                key_order,
                                extra_atts=None,
                                del_atts=None):
        ca, cd, cc, mc = self._partition_keys_for_xml(subtree)
        if extra_atts is not None:
            ca.update(extra_atts)
        if del_atts is not None:
            for da in del_atts:
                if da in ca:
                    del ca[da]
        if self._adding_tree_xsi_type:
            if (key == 'tree') and (parent.tagName == 'trees') and ('xsi:type' not in ca):
                ca['xsi:type'] = 'nex:FloatTree'
        if self._creating_otu_label and (key == 'otu') and (parent.tagName == 'otus'):
            key_to_promote = self.otu_label  # need to verify that we are converting from 1.0 not 0.0..
            # _LOG.debug(str((key_to_promote, mc.keys())))
            if key_to_promote in mc:
                val = mc[key_to_promote]
                if isinstance(val, dict):
                    val = val['$']
                ca['label'] = str(val)
            elif key_to_promote in ca:
                ca['label'] = str(ca[key_to_promote])
        cel = _create_sub_el(doc, parent, key, ca, cd)
        self._add_meta_dict_to_xml(doc, cel, mc)
        self._add_dict_of_subtree_to_xml_doc(doc, cel, cc, key_order)
        return cel

    def _add_meta_dict_to_xml(self, doc, parent, meta_dict):
        """
        Values in the meta element dict are converted to a BadgerFish-style
            encoding (see _convert_hbf_meta_val_for_xml), so regardless of input_format,
            we treat them as if they were BadgerFish.
        """
        if not meta_dict:
            return
        key_list = list(meta_dict.keys())
        key_list.sort()
        for key in key_list:
            el_list = _index_list_of_values(meta_dict, key)
            for el in el_list:
                self._add_meta_value_to_xml_doc(doc, parent, el)

    def _add_meta_value_to_xml_doc(self, doc, parent, obj):
        """Values in the meta element dict are converted to a BadgerFish-style
            encoding (see _convert_hbf_meta_val_for_xml), so regardless of input_format,
            we treat them as if they were BadgerFish.
        """
        return self._add_subtree_to_xml_doc(doc,
                                            parent,
                                            subtree=obj,
                                            key='meta',
                                            key_order=None)


def _nexson_directly_translatable_to_nexml(vers):
    """TEMP: until we refactor nexml writing code to be more general..."""
    return (_is_badgerfish_version(vers)
            or _is_direct_hbf(vers)
            or vers == PhyloSyntax.NEXML)


def strip_to_meta_only(blob, src_syntax):
    if src_syntax is None:
        src_syntax = detect_nexson_version(blob)
    nex = get_nexml_el(blob)
    if _is_by_id_hbf(src_syntax):
        for otus_group in nex.get('otusById', {}).values():
            if 'otuById' in otus_group:
                del otus_group['otuById']
        for trees_group in nex.get('treesById', {}).values():
            tree_group = trees_group['treeById']
            key_list = tree_group.keys()
            for k in key_list:
                tree_group[k] = None
    else:
        otus = nex['otus']
        if not isinstance(otus, list):
            otus = [otus]
        for otus_group in otus:
            if 'otu' in otus_group:
                del otus_group['otu']
        trees = nex['trees']
        if not isinstance(trees, list):
            trees = [trees]
        for trees_group in trees:
            tree_list = trees_group.get('tree')
            if not isinstance(tree_list, list):
                tree_list = [tree_list]
            t = [{'id': i.get('@id')} for i in tree_list]
            trees_group['tree'] = t


def _otu_dict_to_otumap(otu_dict):
    d = {}
    for v in otu_dict.values():
        k = v['^ot:originalLabel']
        mv = d.get(k)
        if mv is None:
            mv = {}
            d[k] = mv
        elif isinstance(mv, list):
            mv.append({})
            mv = mv[-1]
        else:
            mv = [mv, {}]
            mv = mv[-1]
            d[k] = mv
        for mk in ['^ot:ottId', '^ot:ottTaxonName']:
            mvv = v.get(mk)
            if mvv is not None:
                mv[mk] = mvv
    return d


def _convert_bf_meta_val_for_xml(blob):
    if not isinstance(blob, list):
        blob = [blob]
    first_blob = blob[0]
    try:
        try:
            if first_blob.get("@xsi:type") == "nex:LiteralMeta":
                return first_blob["@property"], blob
        except:
            pass
        return first_blob["@rel"], blob
    except:
        return "", blob


def _create_sub_el(doc, parent, tag, attrib, data=None):
    """Creates and xml element for the `doc` with the given `parent`
    and `tag` as the tagName.
    `attrib` should be a dictionary of string keys to primitives or dicts
        if the value is a dict, then the keys of the dict are joined with
        the `attrib` key using a colon. This deals with the badgerfish
        convention of nesting xmlns: attributes in a @xmnls object
    If `data` is not None, then it will be written as data. If it is a boolean,
        the xml true false will be writtten. Otherwise it will be
        converted to python unicode string, stripped and written.
    Returns the element created
    """
    el = doc.createElement(tag)
    if attrib:
        if ('id' in attrib) and ('about' not in attrib):
            about_val = '#' + attrib['id']
            el.setAttribute('about', about_val)
        for att_key, att_value in attrib.items():
            if isinstance(att_value, dict):
                for inner_key, inner_val in att_value.items():
                    rk = ':'.join([att_key, inner_key])
                    el.setAttribute(rk, inner_val)
            else:
                el.setAttribute(att_key, att_value)
    if parent:
        parent.appendChild(el)
    if data is not None:
        if data is True:
            el.appendChild(doc.createTextNode('true'))
        elif data is False:
            el.appendChild(doc.createTextNode('false'))
        else:
            u = UNICODE(data).strip()
            if u:
                el.appendChild(doc.createTextNode(u))
    return el
