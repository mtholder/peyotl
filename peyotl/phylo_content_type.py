#!/usr/bin/env python
# -*- coding: utf-8 -*-
from enum import Enum


class PhyloContentType(Enum):
    FILE = 1
    META = 2
    OTU = 3
    OTU_MAP = 4
    OTUS = 5  # OTUs part of a NexSON doc
    STUDY = 6  # NexSON doc
    SUBTREE = 7
    TREE = 8
    TREE_LIST = 9


_pctstr_to_pct_facet = {
    'file': PhyloContentType.FILE,
    'meta': PhyloContentType.META,
    'otu': PhyloContentType.OTU,
    'otumap': PhyloContentType.OTU_MAP,
    'otus': PhyloContentType.OTUS,
    'study': PhyloContentType.STUDY,
    'subtree': PhyloContentType.SUBTREE,
    'tree': PhyloContentType.TREE,
    'treelist': PhyloContentType.TREE_LIST,
}


def parse_phylo_content_type(arg):
    if isinstance(arg, PhyloContentType):
        return arg
    try:
        _pctstr_to_pct_facet[arg]
    except KeyError:
        raise ValueError('Unrecognized phylo-content-type {}'.format(arg))
