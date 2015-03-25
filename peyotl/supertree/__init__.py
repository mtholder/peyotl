#!/usr/bin/env python
from peyotl.phylo.tree import _TreeWithNodeIDs, parse_newick
from peyotl.phylo.compat import PhyloStatement
from peyotl import get_logger
from enum import Enum
import shutil
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
        if raw_md5_content == full_md5_content:
            _LOG.debug('Subproblem {} unchanged'.format(sub_id))
            return
        self.copy_files('raw', 'stage', OtcArtifact.SUBPROBLEM_INPUT, sub_id)
        subproblem = OtcSupertreeSubproblem(self.stage, sub_id)
        return subproblem.solve_or_simplify(self.solution, self.simple)

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
            print '\n'.join([i.get_newick() for i in in_trees])
            leaf_set = get_leaf_tax_id_set(in_trees)
            print leaf_set
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
        try:
            self._simplification = simplify_subproblem_inputs(with_internals, without_internals)
        except SimplificationError:
            self._status = OtcSupertreeSubproblem.Status.CANNOT_SIMPLIFY_OR_SOLVE
        except:
            self._status = OtcSupertreeSubproblem.Status.CAN_SIMPLIFY
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

