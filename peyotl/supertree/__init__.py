#!/usr/bin/env python
from enum import Enum
from peyotl import get_logger
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
                a_id = line.strip()
                if a_id.endswith('.tre'):
                    a_id = a_id[:-4]
                if a_id:
                    r.append(a_id)
        return r

    def process_raw_subproblem_output(self, sub_id):
        md5filename = sub_id + '.md5'
        raw_md5_content = self.read_filepath('raw', md5filename, True)
        full_md5_content = self.read_filepath('stage', md5filename, False)
        if raw_md5_content == full_md5_content:
            _LOG.debug('Subproblem {} unchanged'.format(sub_id))
            return
        self.copy_files('raw', 'stage', OtcArtifact.SUBPROBLEM_INPUT, sub_id)
        sub_prob = OtcSupertreeSubproblem(self.stage, sub_id)
        if sub_prob.can_solve:
            _LOG.debug('Solving subproblem {}'.format(sub_id))
            sub_prob.write_solution(self.solution)
            return
        if sub_prob.can_simplify:
            _LOG.debug('Simplify subproblem {}'.format(sub_id))
            sub_prob.write_simplification(self.solution)
            return
        _LOG.debug('Cannot solve or simplify subproblem {}'.format(sub_id))

class OtcSupertreeSubproblem(object):
    def __init__(self, input_par, artifact_id):
        self.artifact_id = artifact_id
        self.input_dir = input_par

