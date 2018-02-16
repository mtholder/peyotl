#!/usr/bin/env python
# Some imports to help our py2 code behave like py3
from __future__ import absolute_import, print_function, division

from .utility import (is_str_type,
                      flush_utf_8_writer,
                      get_utf_8_value, get_utf_8_string_io_writer,
                      write_as_json)
from .phylo_content_type import PhyloContentType, parse_phylo_content_type
from .phylo_syntax import (nexson_syntaxes, nexson_version_str,
                           PhyloSyntax, parse_syntax,
                           syntax_to_ext)
from enum import Enum
from codecs import open


def _verify_none_or_bool(val, arg_name):
    if val is None or isinstance(val, bool):
        return val
    raise ValueError('Expecting boolean for {} (if supplied as non-None)'.format(arg_name))


class NodeLabelStyle(Enum):
    ORIGINAL_LABEL = 1
    OTT_ID = 2
    OTT_NAME = 3


def _get_original_label(nd):
    raise NotImplementedError('labeller')


def _get_ott_id(nd):
    raise NotImplementedError('labeller')


def _get_ott_name(nd):
    raise NotImplementedError('labeller')


_label_style_to_labeller = {
    NodeLabelStyle.ORIGINAL_LABEL: _get_original_label,
    NodeLabelStyle.OTT_ID: _get_ott_id,
    NodeLabelStyle.OTT_NAME: _get_ott_name,
}

_label_style_names = {
    'ot:originallabel': NodeLabelStyle.ORIGINAL_LABEL,
    'ot:ottid': NodeLabelStyle.OTT_ID,
    'ot:otttaxonname': NodeLabelStyle.OTT_NAME,
}
_nls_to_str = {v: k for k, v in _label_style_names.items()}


def _parse_otu_label_style(otu_label_style):
    if not isinstance(otu_label_style, NodeLabelStyle):
        ls = otu_label_style.lower()
        otu_label_style = _label_style_names.get(ls)
        if otu_label_style is None:
            pls = 'ot:{}'.format(ls)
            otu_label_style = _label_style_names.get(pls)
            if otu_label_style is None:
                m = 'otu_label_style must be one of: "{}"'
                vlist = [i.name for i in _label_style_to_labeller.keys()]
                raise ValueError(m.format('", "'.join(vlist)))
    return otu_label_style, _label_style_to_labeller[otu_label_style]


class PhyloSchema(object):
    """Simple container for holding the set of variables needed to
    convert from one format to another (with error checking).

    The primary motivation for this class is to:
        1. generate type conversion errors up front when some one requests
            a particular coercion. For example, this allows the phylesystem
            api to raise an error before it fetches the data in cases in which
            the user is requesting a format/content combination is not
            currently supported (or not possible)
        2. allow that agreed-upon coercion to be done later with a simple
            call to convert or serialize. So the class acts like a closure
            that can transform any nexson to the desired format (if NexSON
            has the necessary content)
    """

    # _NEWICK_PROP_VALS = _otu_label2prop.values()
    _no_content_id_types = frozenset([PhyloContentType.META,
                                      PhyloContentType.STUDY,
                                      PhyloContentType.TREE_LIST])
    _tup_content_id_types = {'subtree'}
    _str_content_id_types = frozenset([PhyloContentType.FILE,
                                       PhyloContentType.OTU,
                                       PhyloContentType.OTUS,
                                       PhyloContentType.OTU_MAP,
                                       PhyloContentType.TREE])

    def __init__(self,
                 syntax=None,
                 content=PhyloContentType.STUDY,  # enum facet or string
                 content_id=None,
                 file_ext=None,
                 version=None,
                 bracket_ingroup=False,
                 otu_label_style=None
                 ):
        """ Creates an output specification.

          * `content` specifies the portion of a full study to return. Should be a PhyloContentType
            member or a string version of one of them.
          * `content_id` specifies the id of the element (for those content types that refer to
            a part of a collection (e.g. one OTU)
          * `syntax`, version, file_ext are used to determine the output syntax in the
            following cascade:
              1. If `syntax` is a PhyloSyntax member, it is used.
              2. If `syntax` is not None, but not a PhyloSyntax member then it is treated as
                a string and it plus `version` are used to resolve to a PhyloSyntax.member
              3. `syntax` isparse_phylo_content_type None, then the file_ext must be non-None, and
                it is interpreted as a string specifying the desired file extension.
          * for newick-emitting formats:
             * `bracket_ingroup` can be True, to add [pre-ingroup-marker] and [post-ingroup-marker]
                comments will surround the ingroup
             * `otu_label_style` can be used to specify which information is used to label a node
        """
        self.content = parse_phylo_content_type(content)
        self.content_id = content_id
        if self.content in PhyloSchema._no_content_id_types:
            if self.content_id is not None:
                raise ValueError('No content_id expected for "{}" content'.format(self.content))
        elif self.content in PhyloSchema._str_content_id_types:
            if not (self.content_id is None or is_str_type(self.content_id)):
                m = 'content_id for "{}" content must be a string (if provided)'
                raise ValueError(m.format(self.content))
        else:
            assert self.content == PhyloContentType.SUBTREE
            try:
                assert len(list(self.content_id)) == 2
            except:
                raise ValueError('Expecting 2 content_ids for the "subtree" content')
        self.bracket_ingroup = _verify_none_or_bool(bracket_ingroup, 'bracket_ingroup')
        self.out_syntax = parse_syntax(syntax=syntax,
                                       file_ext=file_ext,
                                       version=version)
        if self.out_syntax not in nexson_syntaxes:
            if self.content == PhyloContentType.META:
                m = 'The "{}" content can only be returned in NexSON'
                raise ValueError(m.format(self.content))
        self.otu_label_style = None
        self.otu_labeller = None
        if otu_label_style is not None:
            ls, lf = _parse_otu_label_style(otu_label_style)
            self.otu_label_style, self.otu_labeller = ls, lf

    @property
    def description(self):
        return self.out_syntax.name

    def can_convert(self):  # pylint: disable=W0613
        if self.out_syntax in nexson_syntaxes:
            return self.content != PhyloContentType.SUBTREE
        if self.content == PhyloContentType.STUDY:
            return True
        if self.content in {PhyloContentType.TREE, PhyloContentType.SUBTREE}:
            return self.out_syntax in [PhyloSyntax.NEWICK, PhyloSyntax.NEXUS]
        return False

    def is_json(self):
        return self.out_syntax in nexson_syntaxes

    def is_xml(self):
        return self.out_syntax == PhyloSyntax.NEXML

    def is_text(self):
        return self.out_syntax in (PhyloSyntax.NEXUS, PhyloSyntax.NEWICK)

    def _phylesystem_api_params(self):
        d = {}
        if self.out_syntax in nexson_syntaxes:
            d['output_nexml2json'] = nexson_version_str(self.out_syntax)
        elif self.otu_label_style != NodeLabelStyle.ORIGINAL_LABEL:
            d['otu_label'] = _nls_to_str[self.otu_label_style]
        return d

    def _phylesystem_api_ext(self):
        return syntax_to_ext[self.out_syntax]

    def phylesystem_api_url(self, base_url, study_id):
        """Returns URL and param dict for a GET call to phylesystem_api
        """
        p = self._phylesystem_api_params()
        e = self._phylesystem_api_ext()
        if self.content == 'study':
            return '{d}/study/{i}{e}'.format(d=base_url, i=study_id, e=e), p
        elif self.content == 'tree':
            if self.content_id is None:
                return '{d}/study/{i}/tree{e}'.format(d=base_url, i=study_id, e=e), p
            return '{d}/study/{i}/tree/{t}{e}'.format(d=base_url, i=study_id, t=self.content_id,
                                                      e=e), p
        elif self.content == 'subtree':
            assert self.content_id is not None
            t, n = self.content_id
            p['subtree_id'] = n
            return '{d}/study/{i}/subtree/{t}{e}'.format(d=base_url, i=study_id, t=t, e=e), p
        elif self.content == 'meta':
            return '{d}/study/{i}/meta{e}'.format(d=base_url, i=study_id, e=e), p
        elif self.content == 'otus':
            if self.content_id is None:
                return '{d}/study/{i}/otus{e}'.format(d=base_url, i=study_id, e=e), p
            return '{d}/study/{i}/otus/{t}{e}'.format(d=base_url, i=study_id, t=self.content_id,
                                                      e=e), p
        elif self.content == 'otu':
            if self.content_id is None:
                return '{d}/study/{i}/otu{e}'.format(d=base_url, i=study_id, e=e), p
            return '{d}/study/{i}/otu/{t}{e}'.format(d=base_url, i=study_id, t=self.content_id,
                                                     e=e), p
        elif self.content == 'otumap':
            return '{d}/otumap/{i}{e}'.format(d=base_url, i=study_id, e=e), p
        else:
            assert False

    def serialize(self, src, output_dest=None, src_syntax=None):
        return self.convert(src, serialize=True, output_dest=output_dest, src_syntax=src_syntax)

    def convert(self, src, serialize=None, output_dest=None, src_syntax=None):
        if not self.can_convert():
            m = 'Conversion of {c} to {d} is not supported'
            raise NotImplementedError(m.format(c=self.content, d=self.description))
        if src_syntax != PhyloSyntax.NEXSON:
            raise NotImplementedError('Only conversion from NexSON is currently supported')
        if self.out_syntax in nexson_syntaxes:
            d = src
            if self.content == PhyloContentType.STUDY:
                d = convert_nexson_format(src,
                                          out_syntax=self.out_syntax,
                                          src_syntax=src_syntax,
                                          remove_old_structs=True,
                                          pristine_if_invalid=False,
                                          sort_arbitrary=False)
            elif self.content in (PhyloContentType.TREE, PhyloContentType.SUBTREE):
                if self.content == PhyloContentType.TREE:
                    d = cull_nonmatching_trees(d, self.content_id, src_syntax=src_syntax)
                    d = convert_nexson_format(d,
                                              out_nexson_format=self.out_syntax,
                                              src_syntax=src_syntax,
                                              remove_old_structs=True,
                                              pristine_if_invalid=False,
                                              sort_arbitrary=False)
                else:
                    i_t_o_list = extract_tree_nexson(d, self.content_id, src_syntax=src_syntax)
                    d = {}
                    for ito_tup in i_t_o_list:
                        i, t = ito_tup[0], ito_tup[1]
                        d[i] = t
            elif self.content == 'meta':
                strip_to_meta_only(d, src_syntax=src_syntax)
            elif self.content == 'otus':
                d = extract_otus_nexson(d, self.content_id, src_syntax=src_syntax)
            elif self.content == 'otu':
                d = extract_otu_nexson(d, self.content_id, src_syntax=src_syntax)
            elif self.content == 'otumap':
                if self.content_id is None:
                    r = extract_otu_nexson(d, None, src_syntax=src_syntax)
                else:
                    p = extract_otus_nexson(d, self.content_id, src_syntax=src_syntax)
                    if p is None:
                        r = extract_otu_nexson(d, self.content_id, src_syntax=src_syntax)
                    else:
                        r = {}
                        for v in p.values():
                            r.update(v.get('otuById', {}))
                if not r:
                    return None
                d = _otu_dict_to_otumap(r)
            elif self.content == 'treelist':
                i_t_o_list = extract_tree_nexson(d,
                                                 self.content_id,
                                                 src_syntax=src_syntax)
                d = [i[0] for i in i_t_o_list]
            if d is None:
                return None
            if serialize:
                if output_dest:
                    write_as_json(d, output_dest)
                    return None
                f, wrapper = get_utf_8_string_io_writer()
                write_as_json(d, wrapper)
                flush_utf_8_writer(wrapper)
                return get_utf_8_value(f)
            return d
        # Non-NexSON types go here...
        if (serialize is not None) and (not serialize):
            raise ValueError(
                'Conversion without serialization is only supported for the NexSON format')
        if output_dest:
            if is_str_type(output_dest):
                output_dest = open(output_dest, 'w', encoding='utf-8')
        if self.out_syntax == PhyloSchema.NEXML:
            if output_dest:
                write_obj_as_nexml(src, output_dest, addindent=' ', newl='\n',
                                   otu_label=self.otu_label_prop)
                return
            return convert_to_nexml(src, addindent=' ', newl='\n', otu_label=self.otu_label_prop)
        elif self.out_syntax in [PhyloSchema.NEXUS, PhyloSchema.NEWICK]:
            if self.content in ('tree', 'subtree'):
                if isinstance(self.content_id, list) or isinstance(self.content_id, tuple):
                    ci, subtree_id = self.content_id
                else:
                    ci, subtree_id = self.content_id, None
            else:
                ci, subtree_id = None, None
            response = extract_tree(src, ci, self, subtree_id=subtree_id)
            # these formats are always serialized...
            if output_dest:
                output_dest.write(response)
                output_dest.write('\n')
            return response
        assert False
