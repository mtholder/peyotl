#!/usr/bin/env python
'''A SimplificationRecord creates (and stores) a stack of noncontroversial
operations to the set of input trees for subproblems of the
maximize-rank-weighted compatability supertree problem.
'''
from peyotl.phylo.tree import _TreeWithNodeIDs
from peyotl.phylo.compat import PhyloStatement, intersection_not_empty
from peyotl import get_logger
from enum import Enum
_LOG = get_logger(__name__)

class SimplifyOp(object):
    class Category(Enum):
        NOT_INCLUDED = 0
        DOMINATED = 1
        TRIVIAL = 2
    def __init__(self, category, statements, leaf_set=None):
        assert isinstance(category, SimplifyOp.Category)
        self.category = category
        self.statements = statements if isinstance(statements, tuple) else tuple(statements)
        if leaf_set is None:
            self.leaf_set = None
        else:
            self.leaf_set = leaf_set if isinstance(leaf_set, frozenset) else frozenset(leaf_set)
class DominatedLabelsSimplifyOp(SimplifyOp):
    def __init__(self, statements, culled):
        _LOG.debug('creating DominatedLabelsSimplifyOp')
        SimplifyOp.__init__(self, SimplifyOp.Category.DOMINATED, statements, culled)
    def roll_back(self, solver):
        _LOG.debug('DominatedLabelsSimplifyOp.roll_back')
        for ps in self.statements:
            solver.attempt_add_statement(ps)


class TrivialStatementSimplifyOp(SimplifyOp):
    def __init__(self, statements):
        _LOG.debug('creating TrivialStatementSimplifyOp')
        SimplifyOp.__init__(self, SimplifyOp.Category.TRIVIAL, statements)
    def roll_back(self, solver):
        _LOG.debug('TrivialStatementSimplifyOp.roll_back')
        for ps in self.statements:
            r = solver.attempt_add_statement(ps)
            assert r
        return True
class GreedySolverFailedError(Exception):
    pass
_EMPTY_TUPLE = tuple()
class TreeBuildingSolver(object):
    def _add_child(self, par, child):
        par.add_child(child)
        assert child.des
        par.des.update(child.des)
    def __init__(self):
        self._bail_on_reject = True
        self.tree = _TreeWithNodeIDs()
        self.tree._root = self.tree.node_type(_id=None)
        self.leaf_set = set()
        self.added_statements = []
        self.redundant_statements = []
        self.rejected_statements = []
        self._unattached_leaves = {}
        self._attached_leaves = {}
    def _add_unattached_leaf(self, label):
        assert(label not in self.leaf_set)
        assert(label not in self._unattached_leaves)
        n = self.tree.node_type(_id=label)
        self.tree._register_node(n)
        self._unattached_leaves[label] = n
        self.leaf_set.add(label)
        n.des = set([label])
        n.ps_list = _EMPTY_TUPLE
    def _add_set_of_unattached_leaves(self, new_labels):
        for label in new_labels:
            self._add_unattached_leaf(label)
    def _find_mrca(self, ps):
        for ll in ps.include:
            nd = self._attached_leaves.get(ll)
            if nd is not None:
                return self._trace_back_to_find_mrca(nd, ps)
        # all inc are unattached
        self._add_set_of_unattached_leaves(ps.include)
        n = self._new_internal_node()
        self._attach_unattached_to_par(n, ps.include)
        return n
    def includes_prohibited(self, nd, ps):
        return intersection_not_empty(nd.des, ps.exclude)
    def _other_ps_allow_nd_as_inc_mrca(self, nps, ps):
        if intersection_not_empty(nps.exclude, ps.include):
            False
        return not intersection_not_empty(nps.include, ps.exclude)
    def _any_other_ps_allow_nd_as_inc_mrca(self, nd, ps):
        for nps in nd.ps_list:
            if not self._other_ps_allow_nd_as_inc_mrca(nps, ps):
                return False
        return True
    def _connected_trace_back_to_find_mrca(self, connected, ps):
        if len(connected) == 1:
            return connected[0]
        nd = connected.pop()
        cids = set([i._id for i in connected])
        cids = cids.intersection(self._attached_leaves.keys())
        while True:
            if cids <= nd.des:
                mrca = nd
                break
            if self.includes_prohibited(nd, ps):
                return None
            #if not self._any_other_ps_allow_nd_as_inc_mrca(nd, ps):
            #    _LOG.debug('_any_other_ps_allow_nd_as_inc_mrca failed for {}'.format(nd.get_newick()))
            #    return None
            if nd._parent is None:
                _LOG.debug(str(cids) + ' is not <= ' + str(nd.des))
                assert False
            nd = nd._parent
            
        checked = set()
        for lin_to_check in connected:
            nd = lin_to_check
            while nd != mrca:
                if nd in checked:
                    break
                checked.add(nd)
                if self.includes_prohibited(nd, ps):
                    return None
            
                #if not self._any_other_ps_allow_nd_as_inc_mrca(nd, ps):
                #    _LOG.debug('_any_other_ps_allow_nd_as_inc_mrca failed for other leaf des{}'.format(nd.get_newick()))
                #    return None
        return mrca
    def _merge_node(self, donor, recipient):
        if donor.is_leaf:
            p = donor._parent
            if p is not None:
                p._remove_child(donor)
            self._add_child(recipient, donor)
        else:
            clist = [i for i in donor.child_iter()]
            for c in clist:
                self._add_child(recipient, c)
            recipient.ps_list.extend(donor.ps_list)
            donor.ps_list = []
            donor._children = []
    def _can_resolve_to_allow(self, nd, ps):
        for c in nd.child_iter():
            if (c.des & ps.include) and (c.des & ps.exclude):
                return False
        return True
    def _steal_children_as_if_resolving(self, nd, nn, ps):
        c_to_move = []
        c_to_retain = []
        for c in nd.child_iter():
            if c.des & ps.include:
                if c.des & ps.exclude:
                    assert False
                c_to_move.append(c)
            else:
                c_to_retain.append(c)
        assert(len(c_to_retain) > 0)
        assert(len(c_to_move) > 1)
        for c in c_to_move:
            nd._remove_child(c)
            self._add_child(nn, c)
        return nd
    def _trace_back_to_find_mrca(self, attached_leaf, ps):
        d = attached_leaf.deepest_anc()
        by_tree_in_forest = {
            d : [attached_leaf],
        }
        u = []
        for ll in ps.include:
            if ll == attached_leaf._id:
                continue
            nd = self._attached_leaves.get(ll)
            if nd is None:
                u.append(nd)
            else:
                d = nd.deepest_anc()
                by_tree_in_forest.setdefault(d, []).append(nd)
        mrcas_by_tree = {}
        internal_mrcas = []
        for tree_root, leaves in by_tree_in_forest.items():
            r = self._connected_trace_back_to_find_mrca(leaves, ps)
            if r is None:
                _LOG.debug('_connected_trace_back_to_find_mrca returning None')
                return None
            mrcas_by_tree[tree_root] = r
            if not r.is_leaf:
                internal_mrcas.append(r)
        w_par = []
        wo_par = []
        for mr in mrcas_by_tree.values():
            if mr._parent is not None:
                #if mr._parent._parent is not None:
                #    _LOG.debug(' first complex case' + mr._parent._parent.get_newick())
                #    raise GreedySolverFailedError() # can't deal with non-trivial forest merges
                w_par.append(mr)
            else:
                wo_par.append(mr)
        needs_new_par = (len(w_par) == 0)
        needs_intervening = []
        does_not_need_intervening = []
        for m in mrcas_by_tree.values():
            if not self._any_other_ps_allow_nd_as_inc_mrca(m, ps):
                if not self._can_resolve_to_allow(m, ps):
                    _LOG.debug('_can_resolve_to_allow False for ' + m.get_newick())
                    return None
                if m._parent is not None:
                    _LOG.debug(' complex case')
                    raise GreedySolverFailedError() # can't deal with non-trivial forest merges
                needs_new_par = True
                needs_intervening.append(m)
            else:
                does_not_need_intervening.append(m)
        if not needs_new_par:
            mrca_ind = None
            for elind, el in enumerate(does_not_need_intervening):
                if not el.is_leaf:
                    mrca_ind = elind
            if mrca_ind is None:
                needs_new_par = True
            else:
                mrca = does_not_need_intervening.pop(mrca_ind)
                assert mrca.des
        if needs_new_par:
            mrca = self._new_internal_node()
        else:
            assert mrca.des
        to_merge_to_par = []
        for m in needs_intervening:
            tmp = self._steal_children_as_if_resolving(m, mrca, ps)
            to_merge_to_par.append(tmp)
        for m in does_not_need_intervening:
            if m._parent is not None:
                to_merge_to_par.append(m._parent)
            self._merge_node(m, mrca)
            assert mrca.des
        assert mrca.des
        if to_merge_to_par:
            fp = mrca._parent
            if fp is None:
                fp =  to_merge_to_par.pop(0)
                self._add_child(fp, mrca)
            for otherp in to_merge_to_par:
                self._merge_node(otherp, fp)
        return mrca

    def _attach_unattached_to_par(self, nd, inc):
        for label in inc:
            c = self._unattached_leaves[label]
            self._attached_leaves[label] = c
            self._add_child(nd, c)
        for label in inc:
            del self._unattached_leaves[label]
        
    def _new_internal_node(self):
        n = self.tree.node_type(_id=None)
        n.des = set()
        n.ps_list = []
        return n
    def attempt_add_statement(self, ps):
        if ps.is_trivial:
            new_labels = ps.leaf_set - self.leaf_set
            if new_labels:
                
                self.added_statements.append(ps)
            else:
                self.redundant_statements.append(ps)
            return True
        _LOG.debug('Solver going to try to add {}'.format(ps.get_newick()))
        m = self._find_mrca(ps)
        if m is None:
            if self._bail_on_reject:
                raise GreedySolverFailedError();
            _LOG.debug('Rejecting for lack of MRCA: {}'.format(ps.get_newick()))
            self.rejected_statements.append(ps)
            return False
        ual = ps.include.difference(self._attached_leaves.keys())
        if ual:
            ucl = ual.difference(self._unattached_leaves.keys())
            self._add_set_of_unattached_leaves(ucl)
            self._attach_unattached_to_par(m, ual)
        _LOG.debug('m.des = ' + str(m.des))
        self._propagate_des_back(m)
        self.added_statements.append(ps)
        m.ps_list.append(ps)
        d = m.deepest_anc()
        _LOG.debug('Attached to: {}'.format(d.get_newick()))
        return True
    def _propagate_des_back(self, nd):
        c = nd._parent
        while c is not None:
            c.des.update(nd.des)
            c = c._parent
    def get_tree_roots(self):
        r = set()
        labels_seen = set()
        n_attach = len(self._attached_leaves)
        for k, n in self._attached_leaves.items():
            if k not in labels_seen:
                d = n.deepest_anc()
                labels_seen.update(d.des)
                r.add(d)
                if len(labels_seen) >= n_attach:
                    break
        return r
    def _could_be_merged(self, x, y):
        '''return True if none of the phylo_statements in x or y exclude the other'''
        for ps in x.ps_list:
            if y.des & ps.exclude:
                return False
        for ps in y.ps_list:
            if x.des & ps.exclude:
                return False
        return True

    def finalize(self):
        tree_roots = self.get_tree_roots()
        if len(tree_roots) > 1:
            checked = set()
            while True:
                mf = False
                mp = None
                for el in tree_roots:
                    if el in checked:
                        continue
                    for sel in tree_roots:
                        if (sel is not el) and (sel not in checked):
                            if self._could_be_merged(el, sel):
                                mp = (el, sel)
                                break
                    if mp is None:
                        checked.add(mp)
                    else:
                        break
                if mp is None:
                    break
                el, sel = mp
                self._merge_roots(el, sel)
                tree_roots.remove(sel)
        if len(tree_roots) > 1:
            self.tree._root = list(tree_roots)[0]
        else:
            if len(tree_roots) > 1:
                assert(self.tree.is_leaf) #odd, that 
                for c in tree_roots:
                    self._add_child(self.tree.root, c)
        self._attach_unattached_to_par(self.tree.root, self._unattached_leaves.keys())
    def _merge_roots(self, recipient, donor):
        clist = [c for c in donor.child_iter()]
        for c in clist:
            donor._remove_child(c)
            self._add_child(recipient, c)


class SimplificationRecord(object):
    def write_statements(self, out):
        for n, el in enumerate(self.statement_list_list):
            out.write('# tree {}\n'.format(self.trees_with_internals[n].source_name))
            for nn, ps in enumerate(el):
                out.write('  {} {}\n'.format(nn, ps.get_newick()))
    def has_simplifications(self):
        return len(self.reductions) > 0
    def can_solve(self):
        if self._solver is False:
            return False
        try:
            tbs = TreeBuildingSolver()
            for row in self.statement_list_list:
                for ps in row:
                    tbs.attempt_add_statement(ps)
            for reduction in self.reductions[-1::-1]:
                reduction.roll_back(tbs)
            tbs.finalize()
        except GreedySolverFailedError:
            self._solver = False
            return False
        self._solver

    def __init__(self, with_internals, without_internals):
        self.reductions = []
        self._solver = None
        self.assume_rank_based = True
        self.trees_with_internals = with_internals
        self.trees_without_internals = without_internals
        self.statement_list_list = []
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
                    _LOG.debug('repeated phylo statement: {}'.format(ps.get_newick()))
                tl.append(tree)
            self.statement_list_list.append(new_phylo_statements)
        if len(in_an_include) < len(self.full_leafset):
            self._cull_exclude_only(in_an_include - self.full_leafset)
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
        f_tree_statements = self.statement_list_list[0]
        some_removed = False
        self.rejected_due_to_conf_w_first = []
        self.rejected_due_to_conf_w_first.append([]) # first tree is self-compatible - it is a tree!
        exit_statement_list_list = [f_tree_statements]
        for tree_st in self.statement_list_list[1:]:
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
            self.statement_list_list = exit_statement_list_list
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

    def _cull_label_set(self, to_cull):
        '''Called by _look_for_dominated to remove a set 
        of labels from all statements.
        '''
        o2n = {}
        nlos = []
        affected_statements = []
        made_trivial = set()
        triv_list = []
        for los in self.statement_list_list:
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
        sop = DominatedLabelsSimplifyOp(affected_statements, to_cull)
        self.reductions.append(sop)
        self.statement_list_list = nlos
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
        for row in self.statement_list_list:
            ind_to_del = []
            for n, el in enumerate(row):
                if el in triv_set:
                    ind_to_del.append(n)
            while ind_to_del:
                n = ind_to_del.pop()
                del row[n]
        sop = TrivialStatementSimplifyOp(triv_list)
        self.reductions.append(sop)
    def _cull_exclude_only(self, in_exclude_only):
        '''Sent set of leaves that are only in exclude statements.
        We can attach these the base of the solution.'''
        self._cull_label_set(in_exclude_only)
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
        '''If, for every statement that includes label `a` there
        exists another label `b` that is on the same "side" (include or exclude side of the statement)
        of each statement, then we say that `a` is dominated by `b`.
        Diagnosing compatibility will not require both `a` and `b`. So we may remove `a` from all 
        statements.
        This is called in a while loop by _look_for_dominated_to_exhaustion
        '''
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
                    else:
                        if id1 in ps.include:
                            joint_with &= set(ps.include)
                        else:
                            assert(id1 in ps.exclude)
                            joint_with &= set(ps.exclude)
                    if len(joint_with) < 2:
                        break
                #_LOG.debug('  joint_with = {}'.format(str(joint_with)))
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
                self._cull_label_set(to_cull)
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
