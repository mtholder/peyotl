#!/usr/bin/env python
from enum import Enum
class UnrootedConflictStatus(Enum):
    EQUIVALENT = 0
    INCOMPATIBLE = 1
    RESOLVES = 2 # compatible with "other" tree, and split is not in that tree
    TRIVIAL = 3 # will be compatible with any taxonomy
    NOT_COMPARABLE = 4 # e.g. lacking an OTT ID in a comparison to taxonomy

class SplitComparison(Enum):
    UNROOTED_INCOMPATIBLE = 0x00 # will conflict on any rooting
                                 # 1 bit is the "unrooted compat" bit
                                 # 2 bit is the "rooted compat" bit
                                 # 4 is the "unroot equiv bit"
                                 # 8 is the "root equiv bit"
    UNROOTED_COMPAT = 0x01 # both could fit on the same rooted tree if one were flipped
    ROOTED_COMPAT = 0x03 # both could fit on the same rooted tree (without altering)
    UNROOTED_EQUIVALENT = 0x07 # rooted differently, but compatible in an unrooted sense
    ROOTED_EQUIVALENT = 0x0F # represent the same partitioning of leaves
def intersection_not_empty(one_set, other):
    if len(one_set) < len(other):
        return any(x in other for x in one_set)
    return any(x in one_set for x in other)
def are_disjoint(one_set, other):
    return not intersection_not_empty(one_set, other)

def sets_are_rooted_compat(one_set, other):
    '''treats the 2 sets are sets of taxon IDs on the same (unstated)
    universe of taxon ids.
    Returns True clades implied by each are compatible and False otherwise
    '''
    if one_set.issubset(other) or other.issubset(one_set):
        return True
    return not intersection_not_empty(one_set, other)

def compare_sets_as_splits(one_set, other, el_universe):
    if one_set.issubset(other) or other.issubset(one_set):
        if one_set == other:
            return SplitComparison.ROOTED_EQUIVALENT
        return SplitComparison.ROOTED_COMPAT
    inter = one_set.intersection(other)
    if not bool(inter):
        if len(one_set) + len(other) == len(el_universe):
            return SplitComparison.UNROOTED_EQUIVALENT
        return SplitComparison.UNROOTED_COMPAT
    if len(one_set) + len(other) - len(inter) == len(el_universe):
        return SplitComparison.UNROOTED_COMPAT
    return SplitComparison.UNROOTED_INCOMPATIBLE

def compare_bits_as_splits(one_set, other, el_universe):
    intersection_b = one_set & other
    if intersection_b == 0:
        union_b = one_set | other
        if el_universe == union_b:
            return SplitComparison.UNROOTED_EQUIVALENT
        return SplitComparison.UNROOTED_COMPAT
    if intersection_b == one_set or intersection_b == other:
        if one_set == other:
            return SplitComparison.ROOTED_EQUIVALENT
        return SplitComparison.ROOTED_COMPAT
    union_b = one_set | other
    if el_universe == union_b:
        return SplitComparison.UNROOTED_COMPAT
    return SplitComparison.UNROOTED_INCOMPATIBLE

class PhyloStatement(object):
    '''This class is defined by just a pair of sets:
        1. A set of "include" and
        2. an "exclude" set of IDs
    These sets must be disjoint.
    The "leaf_set" is the union of `include` and `exclude`
    The biological interpretation of the statement is that the members
        of the include group share at least one common ancestor which is not
        an ancestor of any member of the exclude group.
    A rooted tree can be decomposed into a set of statements by taking
        the cluster of each internal node as the include group in a different
        PhyloStatement.
    '''
    def __init__(self, include, exclude=None, leaf_set=None):
        assert(bool(include))
        self.include = include if isinstance(include, frozenset) else frozenset(include)
        if leaf_set is not None:
            self.leaf_set = leaf_set if isinstance(leaf_set, frozenset) else frozenset(leaf_set)
        if exclude is None:
            assert(leaf_set is not None)
            d = self.leaf_set - self.include
            self.exclude = frozenset(d)
        else:
            self.exclude = exclude if isinstance(exclude, frozenset) else frozenset(exclude)
            assert(self.exclude.isdisjoint(self.include))
            u = self.exclude.union(self.include)
            if leaf_set is None:
                self.leaf_set = u
            else:
                assert(u == self.leaf_set)
    def __eq__(self, other):
        return (self.include == other.include) and (self.leaf_set == other.leaf_set)
    def __neq__(self, other):
        return (self.include != other.include) or (self.leaf_set != other.leaf_set)
    def __hash__(self):
        t = (hash(self.include), hash(self.leaf_set))
        return hash(t)
    def compatible(self, other):
        inter_leaf_set = self.leaf_set - other.leaf_set
        if not inter_leaf_set:
            return True
        eff_inc = inter_leaf_set.intersection(self.include)
        eff_other_inc = inter_leaf_set.intersection(other.include)
        return sets_are_rooted_compat(eff_inc, eff_other_inc)
    def get_newick(self):
        il = list(self.include)
        il.sort()
        el = list(self.exclude)
        el.sort()
        return '(({i}),{e});'.format(i=','.join(il), e=','.join(el))