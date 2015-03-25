#!/usr/bin/env python
from peyotl.phylo.tree import _TreeWithNodeIDs, parse_newick, copy_tree
from peyotl.phylo.compat import PhyloStatement
from peyotl import get_logger
from enum import Enum
import shutil
import sys
import os
_LOG = get_logger(__name__)
class OtcArtifact(Enum):
    SUBPROBLEM_INPUT = 1

_SUBPROBLEM_INPUT_SUFFIXES = ('.tre', '.md5', '-tree-names.txt')

def artifact_to_file_list(artifact_code, artifact_id):
    if artifact_code == OtcArtifact.SUBPROBLEM_INPUT:
        return ['{i}{s}'.format(i=artifact_id, s=s) for s in _SUBPROBLEM_INPUT_SUFFIXES]
    assert isinstance(artifact_code, OtcArtifact)

def copy_set_of_files(src_dir, dest_dir, filename_list):
    for fn in filename_list:
        shutil.copy2(os.path.join(src_dir, fn), os.path.join(dest_dir, fn))
def subproblem_id_from_filename(fn):
    a_id = fn.strip()
    if not a_id:
        return None
    if a_id.endswith('.tre'):
        a_id = a_id[:-4]
    return a_id

class OtcPipelineContext(object):
    '''Convenience class for working with elements of the otcetera supertree
    pipeline.
    Directories:
        'raw' is the output of the otcuncontesteddecompose
        'full' is the full subproblems dir. Every changed subproblem, should be moved here
        'simple' if there an obvious simplification of the subproblem, it should be written here.
        'solution' if the subproblem is trivial (e.g. only one input tree) then the solution should 
                be written here.
    '''
    _DIR_KEYS = ('raw', 'stage', 'simple', 'solution')
    def __init__(self, **kwargs):
        self.raw = kwargs.get('raw_output_dir')
        self.stage = kwargs.get('stage_output_dir')
        self.simple = kwargs.get('simplified_output_dir')
        self.solution = kwargs.get('solution_dir')
        for k in OtcPipelineContext._DIR_KEYS:
            if hasattr(self, k):
                v = getattr(self, k)
                if v is not None:
                    setattr(self, k, os.path.abspath(v))
    def read_filepath(self, dir_type, filename, required=False):
        d = getattr(self, dir_type)
        fp = os.path.join(d, filename)
        if not os.path.isfile(fp):
            if required:
                raise RuntimeError('The required input file "{}" was not found'.format(fp))
            return None
        return open(fp, 'rU').read()
    def copy_files(self, src_tag, dest_tag, artifact_code, artifact_id):
        src = getattr(self, src_tag)
        dest = getattr(self, dest_tag)
        filename_list = artifact_to_file_list(artifact_code, artifact_id)
        copy_set_of_files(src, dest, filename_list)
    def read_artifact_id_list_file(self, artifact_list_fp):
        r = []
        with open(artifact_list_fp, 'rU') as inp:
            for line in inp:
                a_id = subproblem_id_from_filename(line)
                if a_id:
                    r.append(a_id)
        return r
    def process_raw_subproblem_output(self, sub_id):
        '''Updates the subproblem `stage` for subproblem `sub_id`
        if the md5 of this subproblem in the raw output differs
        from the staged content.
        Then analyzes the subproblem to see if it can be solved easily
            or simplified.
        returns a (was_solved, was_simplified) tuple of booleans.
        '''
        md5filename = sub_id + '.md5'
        raw_md5_content = self.read_filepath('raw', md5filename, True)
        full_md5_content = self.read_filepath('stage', md5filename, False)
        _LOG.warn('SKIPPING Copy')
        '''if raw_md5_content == full_md5_content:
            _LOG.debug('Subproblem {} unchanged'.format(sub_id))
            return
        self.copy_files('raw', 'stage', OtcArtifact.SUBPROBLEM_INPUT, sub_id)'''
        subproblem = OtcSupertreeSubproblem(self.stage, sub_id)
        return subproblem.solve_or_simplify(self.solution, self.simple)


def simplify_subproblem_inputs(with_internals, without_internals):
    print '\n'.join([i.source_name for i in with_internals])
    print '\n'.join([i.get_newick() for i in with_internals])
    sr = SimplificationRecord(with_internals, without_internals)
    return sr

class Simplification(Enum):
    REDUNDANT = 1
    TRIVIAL = 2
class SimplificationState(object):
    def __init__(self, cause, list_of_list_of_statements, leaf_set):
        self.cause = cause
        self.list_of_statements = list_of_list_of_statements
        self.leaf_set = leaf_set
class SimplificationRecord(object):
    def write_statements(self, out):
        for n, el in enumerate(self.list_of_statements):
            out.write('# tree {}\n'.format(self.trees_with_internals[n].source_name))
            for nn, ps in enumerate(el):
                out.write('  {} {}\n'.format(nn, ps.get_newick()))
    def has_simplifications(self):
        return len(self.snapshots) > 0
    def __init__(self, with_internals, without_internals):
        self.snapshots = []
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
        self.snapshots.append(SimplificationState(Simplification.REDUNDANT, self.list_of_statements, frozenset(self.full_leafset)))
        o2n = {}
        nlos = []
        list_of_statements = self.list_of_statements
        made_trivial = set()
        triv_list = []
        for los in list_of_statements:
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
                    new_inc = p.include - to_cull
                    nps = PhyloStatement(include=new_inc, leaf_set=rev_ls)
                    if (len(new_inc) < 2) or (new_inc == rev_ls):
                        if nps not in made_trivial:
                            made_trivial.add(nps)
                            triv_list.append(nps)
                    o2n[p] = nps
                    nlos_el.append(nps)
            nlos.append(nlos_el)
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
        self.snapshots.append(SimplificationState(Simplification.TRIVIAL, triv_list, frozenset(self.full_leafset)))
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

class OtcSupertreeSubproblem(object):
    class Status(Enum):
        UNKNOWN = 0
        CAN_SIMPLIFY = 1
        CAN_SOLVE = 2
        CAN_SIMPLIFY_AND_SOLVE = 3
        CANNOT_SIMPLIFY_OR_SOLVE = 4
    def __init__(self, input_dir=None, artifact_id=None, filepath=None):
        '''Takes either:
                `filepath` OR
                `input_dir` AND `artifact_id`
        '''
        if filepath is None:
            self.artifact_id = artifact_id
            self.input_dir = input_dir
            if self.input_dir is None or self.artifact_id is None:
                raise ValueError('artifact_id and input_dir must be specified (or filepath supplied)')
        else:
            self.input_dir, fn = os.path.split(os.path.abspath(filepath))
            aid = subproblem_id_from_filename(fn)
            if aid is None: 
                raise ValueError('Could not parse study ID from filepath = "{}"'.format(filepath))
            if artifact_id is not None and artifact_id != aid:
                raise ValueError('`artifact_id` and `filepath` arguments gave conflicting ids')
            self.artifact_id = aid
        self._input_artifact_filenames = artifact_to_file_list(OtcArtifact.SUBPROBLEM_INPUT, self.artifact_id)
        _input_tree_list = [i for i in self._input_artifact_filenames if i.endswith('.tre')]
        assert(len(_input_tree_list) == 1)
        self._input_tree_filepath = os.path.join(self.input_dir, _input_tree_list[0])
        _input_source_tree_names = [i for i in self._input_artifact_filenames if i.endswith('-tree-names.txt')]
        assert(len(_input_source_tree_names) == 1)
        self._input_source_tree_names_file = os.path.join(self.input_dir, _input_source_tree_names[0])
        self._status = OtcSupertreeSubproblem.Status.UNKNOWN
        self._input_trees = []
        self._solution = None
        self._simplification = None
        self._has_internals = []
    def write_solution(self, solution_dir):
        fn = self.artifact_id + '.tre'
        fp = os.path.join(solution_dir, fn)
        with open(fp, 'w') as outp:
            self._solution.write_newick(outp)
    def solve_or_simplify(self, solution_dir, simplify_dir):
        '''Returns a (was_solved, was_simplified) tuple'''
        if self.can_solve:
            _LOG.debug('Solving subproblem {}'.format(self.artifact_id))
            self.write_solution(solution_dir)
            return True, False
        if self.can_simplify:
            _LOG.debug('Simplify subproblem {}'.format(self.artifact_id))
            self.write_simplification(simplify_dir)
            return False, True
        _LOG.debug('Cannot solve or simplify subproblem {}'.format(self.artifact_id))
        return False, False
    def write_simplification(self, d):
        self._simplification.write_statements(sys.stdout)
    @property
    def can_solve(self):
        if self._status == OtcSupertreeSubproblem.Status.UNKNOWN:
            self._analyze()
        b = self._status.value & OtcSupertreeSubproblem.Status.CAN_SOLVE.value
        return 0 != b
    @property
    def can_simplify(self):
        if self._status == OtcSupertreeSubproblem.Status.UNKNOWN:
            self._analyze()
        b = self._status.value & OtcSupertreeSubproblem.Status.CAN_SIMPLIFY.value
        return 0 != b

    def _analyze(self):
        if not self._input_trees:
            self.read_trees()
        in_trees = self._input_trees
        num_input_trees = len(in_trees)
        if num_input_trees == 1:
            self._solution = in_trees[0]
            self._status = OtcSupertreeSubproblem.Status.CAN_SOLVE
            return
        self._has_internals = [has_nodes_that_make_statements(i) for i in in_trees]
        with_internals = [in_trees[n] for n, hi in enumerate(self._has_internals) if hi]
        if len(with_internals) < 2:
            leaf_set = get_leaf_tax_id_set(in_trees)
            if len(with_internals) == 1:
                soln = copy_tree(with_internals[0])
            else:
                soln = _TreeWithNodeIDs()
                soln._root = soln.node_type(_id=None)
            add_missing_leaves_to_root(soln, leaf_set)
            self._solution = soln
            self._status = OtcSupertreeSubproblem.Status.CAN_SOLVE
            return
        without_internals = [in_trees[n] for n, hi in enumerate(self._has_internals) if not hi]
        print self.artifact_id
        sr = simplify_subproblem_inputs(with_internals, without_internals)
        if sr.has_simplifications():
            self._simplification = sr
            self._status = OtcSupertreeSubproblem.Status.CAN_SIMPLIFY
        else:
            self._status = OtcSupertreeSubproblem.Status.CANNOT_SIMPLIFY_OR_SOLVE
        return
    def read_trees(self):
        if self._input_trees:
            return
        with open(self._input_source_tree_names_file, 'rU') as inp:
            names = [i.strip() for i in inp if i.strip()]
        name_error = False
        with open(self._input_tree_filepath, 'rU') as inp:
            for n, line in enumerate(inp):
                tree = parse_newick(newick=line, _class=_TreeWithNodeIDs)
                try:
                    tree.source_name = names[n]
                except:
                    name_error = True
                    break
                self._input_trees.append(tree)
        if name_error or (len(names) != len(self._input_trees)):
            m = 'The number of trees in "{}" differed from the number of tree names in "{}"'
            m = m.format(self._input_tree_filepath, self._input_source_tree_names_file)
            raise RuntimeError(m)

def get_leaf_tax_id_set(tree_list):
    ls = set()
    for tree in tree_list:
        tls = tree.leaf_id_set()
        ls.update(tls)
    return ls
def has_nodes_that_make_statements(tree):
    return tree.has_nodes_that_make_statements()

def add_missing_leaves_to_root(tree, leaf_set):
    tls = tree.leaf_id_set()
    to_add = leaf_set - tls
    if to_add:
        root = tree.root
        for tax_id in to_add:
            tree.add_new_child(root, tax_id)

