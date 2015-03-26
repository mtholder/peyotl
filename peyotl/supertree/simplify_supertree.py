#!/usr/bin/env python
'''A SimplificationRecord creates (and stores) a stack of noncontroversial
operations to the set of input trees for subproblems of the
maximize-rank-weighted compatability supertree problem.
'''
from peyotl.phylo.compat import PhyloStatement
from peyotl import get_logger
from enum import Enum
_LOG = get_logger(__name__)


class SimplifyOp(object):
    class Category(Enum):
        NOT_INCLUDED = 0
        DOMINATED = 1
        TRIVIAL = 2
    def __init__(self, category, statements, leaf_set):
        assert isinstance(category, SimplifyOp.Category)
        self.category = category
        self.statements = statements if isinstance(statements, tuple) else tuple(statements)
        self.leaf_set = leaf_set if isinstance(leaf_set, frozenset) else frozenset(leaf_set)
class SimplificationRecord(object):
    def write_statements(self, out):
        for n, el in enumerate(self.list_of_statements):
            out.write('# tree {}\n'.format(self.trees_with_internals[n].source_name))
            for nn, ps in enumerate(el):
                out.write('  {} {}\n'.format(nn, ps.get_newick()))
    def has_simplifications(self):
        return len(self.reductions) > 0
    def __init__(self, with_internals, without_internals):
        self.reductions = []
        self.assume_rank_based = True
        self.trees_with_internals = with_internals
        self.trees_without_internals = without_internals
        self.list_of_statements = []
        self.ps2trees = {}
        self.full_leafset = set()
        in_an_include = set()
        for tree in with_internals:
            phylo_statements = tree.get_phylo_statements()
            leaves_without_statements = tree.get_leaf_ids_with_no_phylo_statements()
            if leaves_without_statements:
                rls = set(phylo_statements[0].leaf_set)
                rls -= leaves_without_statements
                in_an_include |= rls
            self.full_leafset.update(phylo_statements[0].leaf_set)
            new_phylo_statements = []
            for ps in phylo_statements:
                tl = self.ps2trees.setdefault(ps, [])
                if not tl:
                    new_phylo_statements.append(ps)
                else:
                    _LOG.debug('repeated phylo statement')
                tl.append(tree)
            self.list_of_statements.append(new_phylo_statements)
        if len(in_an_include) < len(self.full_leafset):
            self.cull_exclude_only(in_an_include - self.full_leafset)
        self.redundancy_checked = set()
        self.simplify_to_exhaustion()
    def simplify_to_exhaustion(self):
        c, r = True, False
        if self.assume_rank_based:
            r = self._remove_conflicting_with_first_tree()
        while c:
            c = self._look_for_dominated_to_exhaustion()
            if c:
                r = True
        return r
    def _remove_conflicting_with_first_tree(self):
        self.redundancy_checked = set()
        f_tree_statements = self.list_of_statements[0]
        some_removed = False
        self.rejected_due_to_conf_w_first = []
        self.rejected_due_to_conf_w_first.append([]) # first tree is self-compatible - it is a tree!
        exit_statement_list_list = [f_tree_statements]
        for tree_st in self.list_of_statements[1:]:
            retained = []
            rejected = []
            for ps in tree_st:
                was_rejected = False
                for fs in f_tree_statements:
                    if not fs.compatible(ps):
                        rejected.append(ps)
                        was_rejected = True
                        some_removed = True
                        break
                if not was_rejected:
                    retained.append(ps)
            exit_statement_list_list.append(retained)
        if some_removed:
            self.list_of_statements = exit_statement_list_list
        return some_removed
    def _look_for_dominated_to_exhaustion(self):
        r = True
        some_simplification = False
        while r:
            full_leaf_id_list = list(self.full_leafset)
            full_leaf_id_list.sort()
            r = self._look_for_dominated(full_leaf_id_list)
            if r:
                some_simplification = True
        return some_simplification
    def _cull_dominated(self, to_cull):
        o2n = {}
        nlos = []
        affected_statements = []
        made_trivial = set()
        triv_list = []
        for los in self.list_of_statements:
            if not los:
                nlos.append([])
                continue
            isect = los[0].leaf_set.intersection(to_cull)
            nlos_el = []
            if not isect:
                for p in los:
                    nps = PhyloStatement(p.include, leaf_set=p.leaf_set)
                    o2n[p] = nps
                    nlos_el.append(nps)
            else:
                rev_ls = frozenset(los[0].leaf_set - isect)
                for p in los:
                    affected_statements.append(p)
                    new_inc = p.include - to_cull
                    nps = PhyloStatement(include=new_inc, leaf_set=rev_ls)
                    if (len(new_inc) < 2) or (new_inc == rev_ls):
                        if nps not in made_trivial:
                            made_trivial.add(nps)
                            triv_list.append(nps)
                    o2n[p] = nps
                    nlos_el.append(nps)
            nlos.append(nlos_el)
        sop = SimplifyOp(SimplifyOp.Category.DOMINATED,
                         affected_statements,
                         self.full_leafset)
        self.reductions.append(sop)
        self.list_of_statements = nlos
        ps2trees = {}
        for k, v in self.ps2trees.items():
            nk = o2n[k]
            ps2trees[nk] = v
        self.ps2trees = ps2trees
        self.full_leafset -= to_cull
        if made_trivial:
            self._cull_trivial(triv_list, made_trivial)
    def _cull_trivial(self, triv_list, triv_set):
        for t in triv_list:
            del self.ps2trees[t]
        for row in self.list_of_statements:
            ind_to_del = []
            for n, el in enumerate(row):
                if el in triv_set:
                    ind_to_del.append(n)
            while ind_to_del:
                n = ind_to_del.pop()
                del row[n]
        self.reductions.append(SimplifyOp(SimplifyOp.Category.TRIVIAL, triv_list, frozenset(self.full_leafset)))
    def _is_dominated_by(self, one_id, another):
        for ps in self.ps2trees.keys():
            if one_id in ps.leaf_set:
                if another not in ps.leaf_set:
                    return False
                if one_id in ps.include:
                    if another not in ps.include:
                     return False
                else:
                    assert one_id in ps.exclude
                    if another not in ps.exclude:
                        return False
        return True

    def _look_for_dominated(self, full_leaf_id_list):
        for id1 in full_leaf_id_list:
            if id1 in self.redundancy_checked:
                continue
            _LOG.debug('Checking {} for domination'.format(id1))
            joint_with = None
            for ps in self.ps2trees.keys():
                if id1 in ps.leaf_set:
                    if joint_with is None:
                        if id1 in ps.include:
                            joint_with = set(ps.include)
                        else:
                            assert(id1 in ps.exclude)
                            joint_with = set(ps.exclude)
                        assert(len(joint_with) > 1)
                    else:
                        if id1 in ps.include:
                            joint_with &= set(ps.include)
                        else:
                            assert(id1 in ps.exclude)
                            joint_with &= set(ps.exclude)
                        if len(joint_with) < 2:
                            break
                _LOG.debug('  joint_with = {}'.format(str(joint_with)))
            if (joint_with is not None) and (len(joint_with) > 1):
                assert(id1 in joint_with)
                dom_by = None
                for el in joint_with:
                    if el != id1:
                        dom_by = el
                        break
                assert(dom_by != None)
                to_cull = set([id1])
                for el in joint_with:
                    if (el is id1) or (el is dom_by):
                        continue
                    if self._is_dominated_by(el, dom_by):
                        to_cull.add(el)
                _LOG.debug('Culling {}'.format(to_cull))
                self._cull_dominated(to_cull)
                self.redundancy_checked.update(to_cull)
                return True
            self.redundancy_checked.add(id1)


def simplify_subproblem_inputs(with_internals, without_internals):
    '''Takes 2 list of trees. The first should all have internal 
    nodes (other than the root). The second list should just be trees
    that have tips and the root only.
    An new SimplificationRecord is returned.
    '''
    print '\n'.join([i.source_name for i in with_internals])
    print '\n'.join([i.get_newick() for i in with_internals])
    sr = SimplificationRecord(with_internals, without_internals)
    return sr
