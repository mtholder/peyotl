import os
import re
import json
import codecs
from threading import Lock
from peyotl.utility import get_config_setting, get_logger
from peyotl.git_storage.git_shard import (GitShardProxy,
                                          TypeAwareGitShard)
from peyotl.phylesystem.git_workflows import validate_and_convert_nexson
from peyotl.nexson_syntax import PhyloSchema
from peyotl.phylesystem.git_actions import PhylesystemFilepathMapper
from peyotl.phylesystem.helper import create_id2study_info
from peyotl.phylesystem.git_actions import PhylesystemGitAction

_LOG = get_logger(__name__)


class NexsonDocSchema(object):
    optional_output_detail_keys = ('tip_label', 'bracket_ingroup')

    def __init__(self, schema_version='1.2.1'):
        self.schema_version = schema_version
        self.document_type = 'study'
        self.schema_name = 'NexSON'

    def __repr__(self):
        return 'NexsonDocSchema(schema_version={})'.format(self.schema_version)

    def is_plausible_transformation_or_raise(self, subresource_request):
        """See TypeAwareDocStore._is_plausible_transformation_or_raise
        """
        _LOG.debug('phylesystem.is_plausible_transformation({})'.format(subresource_request))
        sub_res_set = {'meta', 'tree', 'subtree', 'otus', 'otu', 'otumap', 'file'}
        rt = subresource_request.get('subresource_type')
        if rt:
            if rt not in sub_res_set:
                return False, 'extracting "{}" out of a study is not supported.'.format(rt), None
        else:
            rt = 'study'
        si = subresource_request.get('subresource_id')
        out_fmt_dict = subresource_request.get('output_format')
        if not out_fmt_dict:
            out_fmt_dict = {}

        schema_name = out_fmt_dict.get('schema')
        type_ext = out_fmt_dict.get('type_ext')
        schema_version = out_fmt_dict.get('schema_version')
        detail_kwargs = {}
        for k in self.optional_output_detail_keys:
            x = out_fmt_dict.get(k)
            if x is not None:
                detail_kwargs[k] = x

        schema = PhyloSchema(schema=schema_name,
                             content=rt,
                             content_id=si,
                             output_nexml2json=schema_version,
                             repo_nexml2json=self.schema_version,
                             type_ext=type_ext,
                             **detail_kwargs)
        subresource_request['output_is_json'] = schema.output_is_json
        if not schema.can_convert_from():
            msg = 'Cannot convert from {s} to {d}'.format(s=self.schema_version,
                                                          d=schema.description)
            return False, msg, None
        syntax_str = schema.syntax_type
        if rt == 'study' and schema.output_is_json:
            def annotate_and_transform_closure(doc_store_umbrella, doc_id, document_obj, head_sha):
                blob_sha = doc_store_umbrella.get_blob_sha_for_study_id(doc_id, head_sha)
                _LOG.debug('doc_obj.keys() = {}'.format(document_obj.keys()))
                doc_store_umbrella.add_validation_annotation(document_obj, blob_sha)
                return schema.convert(document_obj)

            return True, annotate_and_transform_closure, syntax_str

        else:
            def transform_closure(doc_store_umbrella, doc_id, document_obj, head_sha):
                return schema.convert(document_obj)

            return True, transform_closure, syntax_str

    def validate_annotate_convert_doc(self, document, **kwargs):
        """Adaptor between exception-raising validate_and_convert_nexson and generic interface"""
        try:
            bundle = validate_and_convert_nexson(document,
                                                 self.schema_version,
                                                 allow_invalid=False,
                                                 max_num_trees_per_study=kwargs.get('max_num_trees_per_study'))
            converted_nexson = bundle[0]
            annotation = bundle[1]
            nexson_adaptor = bundle[3]
        except GitWorkflowError as err:
            return document, [err.msg or 'No message found'], None, None
        return converted_nexson, [], annotation, nexson_adaptor


class PhylesystemShardProxy(GitShardProxy):
    """Proxy for shard when interacting with external resources if given the configuration of a remote Phylesystem
    """

    def __init__(self, config):
        GitShardProxy.__init__(self, config, 'studies', 'study', doc_schema=NexsonDocSchema)

    # rename some generic members in the base class, for clarity and backward compatibility
    @property
    def repo_nexml2json(self):
        return self.doc_schema.schema_version

    @property
    def study_index(self):
        return self.doc_index

    @study_index.setter
    def study_index(self, val):
        self._doc_index = val

    @property
    def new_study_prefix(self):
        return self.new_doc_prefix

    def get_study_ids(self):
        return self.get_doc_ids()


def _diagnose_repo_nexml2json(shard):
    """Optimistic test for Nexson version in a shard (tests first study found)"""
    with shard._index_lock:
        fp = next(iter(shard.study_index.values()))[2]
    with codecs.open(fp, mode='r', encoding='utf-8') as fo:
        fj = json.load(fo)
        from peyotl.nexson_syntax import detect_nexson_version
        shard.doc_schema.schema_version = detect_nexson_version(fj)
        return


def refresh_study_index(shard, initializing=False):
    d = create_id2study_info(shard.doc_dir, shard.name)
    shard.has_aliases = False
    shard.study_index = d


class PhylesystemShard(TypeAwareGitShard):
    """Wrapper around a git repo holding nexson studies.
    Raises a ValueError if the directory does not appear to be a PhylesystemShard.
    Raises a RuntimeError for errors associated with misconfiguration."""
    document_type = 'study'

    def __init__(self,
                 name,
                 path,
                 push_mirror_repo_path=None,
                 infrastructure_commit_author='OpenTree API <api@opentreeoflife.org>',
                 max_file_size=None):
        if max_file_size is None:
            max_file_size = get_config_setting('phylesystem', 'max_file_size')
        TypeAwareGitShard.__init__(self,
                                   name=name,
                                   path=path,
                                   doc_holder_subpath='study',
                                   doc_schema=NexsonDocSchema(),
                                   refresh_doc_index_fn=refresh_study_index,  # populates 'study_index'
                                   git_action_class=PhylesystemGitAction,
                                   push_mirror_repo_path=push_mirror_repo_path,
                                   infrastructure_commit_author=infrastructure_commit_author,
                                   max_file_size=max_file_size)
        self._id_minting_file = os.path.join(path, 'next_study_id.json')
        self._next_study_id = None
        # _diagnose_repo_nexml2json(self) # needed if we return to supporting >1 NexSON version in a repo

    def can_mint_new_docs(self):
        return self._new_doc_prefix is not None

    # rename some generic members in the base class, for clarity and backward compatibility
    @property
    def next_study_id(self):
        return self._next_study_id

    @property
    def iter_study_objs(self):
        return self.iter_doc_objs

    @property
    def iter_study_filepaths(self):
        return self.iter_doc_filepaths

    @property
    def get_changed_studies(self):
        return self.get_changed_docs

    @property
    def known_prefixes(self):
        if self._known_prefixes is None:
            self._known_prefixes = self._diagnose_prefixes()
        return self._known_prefixes

    @property
    def new_study_prefix(self):
        return self._new_doc_prefix

    @property
    def study_index(self):
        return self.doc_index

    @study_index.setter
    def study_index(self, val):
        self._doc_index = val

    @property
    def repo_nexml2json(self):
        return self.doc_schema.schema_version

    def get_study_ids(self):
        return self.get_doc_ids()

    def _determine_next_study_id(self):
        """Return the numeric part of the newest study_id

        Checks out master branch as a side effect!
        """
        prefix = self._new_doc_prefix
        lp = len(prefix)
        n = 0
        # this function holds the lock for quite awhile,
        #   but it only called on the first instance of
        #   of creating a new study
        with self._doc_counter_lock:
            with self._index_lock:
                for k in self.study_index.keys():
                    if k.startswith(prefix):
                        try:
                            pn = int(k[lp:])
                            if pn > n:
                                n = pn
                        except:
                            pass
            nsi_contents = self._read_master_branch_resource(self._id_minting_file, is_json=True)
            try:
                self._next_study_id = nsi_contents['next_study_id']
            except:
                raise RuntimeError("Could not read 'next_study_id' from {}".format(self._id_minting_file))
            if self._next_study_id <= n:
                m = 'next_study_id in {} is set lower than the ID of an existing study!'
                m = m.format(self._id_minting_file)
                raise RuntimeError(m)

    def _advance_new_study_id(self):
        """ ASSUMES the caller holds the _doc_counter_lock !
        Returns the current numeric part of the next study ID, advances
        the counter to the next value, and stores that value in the
        file in case the server is restarted.
        """
        c = self._next_study_id
        self._next_study_id = 1 + c
        content = u'{"next_study_id": %d}\n' % self._next_study_id
        # The content is JSON, but we hand-rolled the string above
        #       so that we can use it as a commit_msg
        self._write_master_branch_resource(content,
                                           self._id_minting_file,
                                           commit_msg=content,
                                           is_json=False)
        return c

    def _diagnose_prefixes(self):
        """Returns a set of all of the prefixes seen in the main document dir
        """
        p = set()
        for name in os.listdir(self.doc_dir):
            if PhylesystemFilepathMapper.id_pattern.match(name):
                p.add(name[:3])
        return p

    def _mint_new_study_id(self):
        """Checks out master branch as a side effect"""
        # studies created by the OpenTree API start with ot_,
        # so they don't conflict with new study id's from other sources
        with self._doc_counter_lock:
            c = self._advance_new_study_id()
        # @TODO. This form of incrementing assumes that
        #   this codebase is the only service minting
        #   new study IDs!
        return "{p}{c:d}".format(p=self._new_doc_prefix, c=c)

    def create_git_action_for_new_study(self, new_study_id=None):
        """Checks out master branch as a side effect"""
        ga = self.create_git_action()
        if new_study_id is None:
            new_study_id = self._mint_new_study_id()
        self.register_doc_id(ga, new_study_id)
        return ga, new_study_id
