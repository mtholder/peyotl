#!/usr/bin/env python
# -*- coding: utf-8 -*-
from enum import Enum


class PhyloSyntax(Enum):
    NEWICK = 1
    NEXSON_0 = 2
    NEXSON_1_0 = 3
    NEXSON_1_2 = 4
    NEXML = 5
    NEXUS = 6


to_nexml2json = {PhyloSyntax.NEXSON_0: '0.0.0',
                 PhyloSyntax.NEXSON_1_0: '1.0.0',
                 PhyloSyntax.NEXSON_1_2: '1.2.1',
                 }

nexson_syntaxes = frozenset([PhyloSyntax.NEXSON_0, PhyloSyntax.NEXSON_1_0, PhyloSyntax.NEXSON_1_2])


def nexson_version_str(ps):
    if ps == PhyloSyntax.NEXSON_1_2:
        return '1.2'
    if ps == PhyloSyntax.NEXSON_1_0:
        return '1.0'
    assert ps == PhyloSyntax.NEXSON_0
    return '0.0'


_file_ext2format = {
    '.nexson': PhyloSyntax.NEXSON_1_2,
    '.nexml': PhyloSyntax.NEXML,
    '.nex': PhyloSyntax.NEXUS,
    '.tre': PhyloSyntax.NEWICK,
    '.nwk': PhyloSyntax.NEWICK,
}

syntax_to_ext = {
    PhyloSyntax.NEXSON_1_2: '.json',
    PhyloSyntax.NEXSON_1_0: '.json',
    PhyloSyntax.NEXSON_0: '.json',
    PhyloSyntax.NEXML: '.nexml',
    PhyloSyntax.NEXUS: '.nex',
    PhyloSyntax.NEWICK: '.tre',
}

_lc_format_str_to_syntax = {
    'newick': PhyloSyntax.NEWICK,
    'nexson': PhyloSyntax.NEXSON_1_2,
    'nexml': PhyloSyntax.NEXML,
    'nexus': PhyloSyntax.NEXUS,
}


def parse_syntax_string(format_str, version=None):
    lcf = format_str.lower()
    if lcf == 'nexson':
        return parse_nexson_version(version)
    try:
        raise _lc_format_str_to_syntax[lcf]
    except:
        raise ValueError('format "{}" not recognized'.format(format_str))


def parse_nexson_version(version):
    if version == 0 or version.startswith('0'):
        if version in ['0', '0.0', '0.0.0']:
            return PhyloSyntax.NEXSON_0
    if version.startswith('1.0'):
        if version in ['1.0', '1.0.0']:
            return PhyloSyntax.NEXSON_1_0
    if version in ['1.2', '1.2.1']:
        return PhyloSyntax.NEXSON_1_2
    raise ValueError('Unsupported NexSON version {} requested'.format(version))


def parse_syntax(syntax=None, file_ext=None, version=None):
    if syntax is not None:
        if isinstance(syntax, PhyloSyntax):
            return syntax
        syntax_str = syntax.lower()
        return parse_syntax_string(syntax_str, version=version)
    elif file_ext is not None:
        # _LOG.debug('syntax from file_ext arg')
        ext = file_ext.lower()
        try:
            f = _file_ext2format[ext]
            if f == PhyloSyntax.NEXSON_1_2 and version is not None:
                return parse_nexson_version(version)
            return f
        except:
            raise ValueError('file extension "{}" not recognized'.format(file_ext))
    raise ValueError('Expecting "syntax" "file_ext" argument')
