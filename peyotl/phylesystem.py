import os
import re
import json
import codecs
from threading import Lock
from peyotl.utility.input_output import write_as_json
from peyotl.git_storage.git_shard import (GitShardProxy,
                                          TypeAwareGitShard)
from peyotl.nexson_syntax import PhyloSchema
from peyotl.git_storage.git_action import GitWorkflowError
from peyotl.nexson_syntax import convert_nexson_format
import traceback
from peyotl.utility import get_logger, get_config_setting

try:
    # noinspection PyPackageRequirements,PyUnresolvedReferences
    from dogpile.cache.api import NO_VALUE
except:
    pass  # caching is optional
from peyotl.git_storage import (ShardedDocStore, ShardedDocStoreProxy, TypeAwareDocStore,
                                get_phylesystem_repo_parent)
from peyotl.nexson_validation import ot_validate
from peyotl.nexson_validation._validation_base import NexsonAnnotationAdder, replace_same_agent_annotation

_LOG = get_logger(__name__)
_study_index_lock = Lock()


def create_id2study_info(path, tag):
    """Searchers for *.json files in this repo and returns
    a map of study id ==> (`tag`, dir, study filepath)
    where `tag` is typically the shard name
    """
    d = {}
    for triple in os.walk(path):
        root, files = triple[0], triple[2]
        for filename in files:
            if filename.endswith('.json'):
                study_id = filename[:-5]
                d[study_id] = (tag, root, os.path.join(root, filename))
    return d


DIGIT_PATTERN = re.compile(r'^\d')

_CACHE_REGION_CONFIGURED = False
_REGION = None


def _make_phylesystem_cache_region():
    """Only intended to be called by the Phylesystem singleton.
    """
    global _CACHE_REGION_CONFIGURED, _REGION
    if _CACHE_REGION_CONFIGURED:
        return _REGION
    _CACHE_REGION_CONFIGURED = True
    try:
        # noinspection PyPackageRequirements,PyUnresolvedReferences
        from dogpile.cache import make_region
    except:
        _LOG.debug('dogpile.cache not available')
        return
    trial_key = 'test_key'
    trial_val = {'test_val': [4, 3]}
    trying_redis = True
    if trying_redis:
        try:
            a = {
                'host': 'localhost',
                'port': 6379,
                'db': 0,  # default is 0
                'redis_expiration_time': 60 * 60 * 24 * 2,  # 2 days
                'distributed_lock': False  # True if multiple processes will use redis
            }
            region = make_region().configure('dogpile.cache.redis', arguments=a)
            _LOG.debug('cache region set up with cache.redis.')
            _LOG.debug('testing redis caching...')
            region.set(trial_key, trial_val)
            assert trial_val == region.get(trial_key)
            _LOG.debug('redis caching works')
            region.delete(trial_key)
            _REGION = region
            return region
        except:
            _LOG.debug('redis cache set up failed.')
    trying_file_dbm = False
    if trying_file_dbm:
        _LOG.debug('Going to try dogpile.cache.dbm ...')
        first_par = get_phylesystem_repo_parent()
        cache_db_dir = os.path.split(first_par)[0]
        cache_db = os.path.join(cache_db_dir, 'phylesystem-cachefile.dbm')
        _LOG.debug('dogpile.cache region using "{}"'.format(cache_db))
        try:
            a = {'filename': cache_db}
            region = make_region().configure('dogpile.cache.dbm',
                                             expiration_time=36000,
                                             arguments=a)
            _LOG.debug('cache region set up with cache.dbm.')
            _LOG.debug('testing anydbm caching...')
            region.set(trial_key, trial_val)
            assert trial_val == region.get(trial_key)
            _LOG.debug('anydbm caching works')
            region.delete(trial_key)
            _REGION = region
            return region
        except:
            _LOG.debug('anydbm cache set up failed')
            _LOG.debug('exception in the configuration of the cache.')
    _LOG.debug('Phylesystem will not use caching')
    return None


TRACE_FILES = False


def _write_to_next_free(tag, blob):
    """#WARNING not thread safe just a easy of debugging routine!"""
    ind = 0
    pref = '/tmp/peyotl-' + tag + str(ind)
    while os.path.exists(pref):
        ind += 1
        pref = '/tmp/peyotl-' + tag + str(ind)
    write_as_json(blob, pref)


def validate_and_convert_nexson(nexson, output_version, allow_invalid, **kwargs):
    """Runs the nexson validator and returns a converted 4 object:
        nexson, annotation, validation_log, nexson_adaptor

    `nexson` is the nexson dict.
    `output_version` is the version of nexson syntax to be used after validation.
    if `allow_invalid` is False, and the nexson validation has errors, then
        a GitWorkflowError will be generated before conversion.
    """
    try:
        if TRACE_FILES:
            _write_to_next_free('input', nexson)
        annotation, validation_log, nexson_adaptor = ot_validate(nexson, **kwargs)
        if TRACE_FILES:
            _write_to_next_free('annotation', annotation)
    except:
        msg = 'exception in ot_validate: ' + traceback.format_exc()
        raise GitWorkflowError(msg)
    if (not allow_invalid) and validation_log.has_error():
        raise GitWorkflowError('ot_validation failed: ' + json.dumps(annotation))
    nexson = convert_nexson_format(nexson, output_version)
    if TRACE_FILES:
        _write_to_next_free('converted', nexson)
    return nexson, annotation, validation_log, nexson_adaptor


# noinspection PyMethodMayBeStatic
class PhylesystemFilepathMapper(object):
    id_pattern = re.compile(r'[a-zA-Z][a-zA-Z]_[0-9]+')
    wip_id_template = '.*_study_{i}_[0-9]+'
    branch_name_template = "{ghu}_study_{rid}"
    path_to_user_splitter = '_study_'
    doc_holder_subpath = 'study'
    doc_parent_dir = 'study/'

    def filepath_for_id(self, repo_dir, study_id):
        assert len(study_id) >= 4
        assert study_id[2] == '_'
        assert bool(PhylesystemFilepathMapper.id_pattern.match(study_id))
        frag = study_id[-2:]
        dest_topdir = study_id[:3] + frag
        dest_subdir = study_id
        dest_file = dest_subdir + '.json'
        return os.path.join(repo_dir, 'study', dest_topdir, dest_subdir, dest_file)

    def id_from_rel_path(self, path):
        if path.startswith('study/'):
            try:
                p = path.split('/')[-2]
            except:
                return None
            if p.endswith('.json'):
                return p[:-5]
            return p

    def prefix_from_doc_id(self, doc_id):
        # TODO: Use something smarter here, splitting on underscore?
        return doc_id[:3]


phylesystem_path_mapper = PhylesystemFilepathMapper()


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
            # noinspection PyUnusedLocal
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
        GitShardProxy.__init__(self, config, 'studies',
                               path_mapper=phylesystem_path_mapper, doc_schema=NexsonDocSchema)

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


def refresh_study_index(shard):
    d = create_id2study_info(shard.doc_dir, shard.name)
    shard.has_aliases = False
    shard.study_index = d


class PhylesystemShard(TypeAwareGitShard):
    """Wrapper around a git repo holding nexson studies.
    Raises a ValueError if the directory does not appear to be a PhylesystemShard.
    Raises a RuntimeError for errors associated with misconfiguration."""

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
                                   doc_schema=NexsonDocSchema(),
                                   push_mirror_repo_path=push_mirror_repo_path,
                                   infrastructure_commit_author=infrastructure_commit_author,
                                   max_file_size=max_file_size,
                                   path_mapper=phylesystem_path_mapper)
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

    def check_new_doc_id(self, new_doc_id):
        if new_doc_id is None:
            return self._mint_new_study_id()
        return new_doc_id


class PhylesystemProxy(ShardedDocStoreProxy):
    """Proxy for interacting with external resources if given the configuration of a remote Phylesystem.
    N.B. that this has minimal functionality, and is mainly used to fetch studies and their ids.
    """

    def __init__(self, config):
        ShardedDocStore.__init__(self, path_mapper=phylesystem_path_mapper)
        self._shards = []
        for s in config.get('shards', []):
            self._shards.append(PhylesystemShardProxy(s))
        self._doc2shard_map = None
        self.create_doc_index('study')


class _Phylesystem(TypeAwareDocStore):
    """Wrapper around a set of sharded git repos, with business rules specific to Nexson studies.
    """
    id_regex = PhylesystemFilepathMapper.id_pattern

    def __init__(self,
                 repos_dict=None,
                 repos_par=None,
                 mirror_info=None,
                 infrastructure_commit_author='OpenTree API <api@opentreeoflife.org>',
                 shard_mirror_pair_list=None,
                 with_caching=True):
        """
        Repos can be found by passing in a `repos_par` (a directory that is the parent of the repos)
            or by trusting the `repos_dict` mapping of name to repo filepath.
        `with_caching` should be True for non-debugging uses.
        If you want to use a mirrors of the repo for pushes or pulls, send in a `mirror_info` dict:
            mirror_info['push'] and mirror_info['pull'] should be dicts with the following keys:
            'parent_dir' - the parent directory of the mirrored repos
            'remote_map' - a dictionary of remote name to prefix (the repo name + '.git' will be
                appended to create the URL for pushing).
        """
        self._new_doc_prefix = None
        TypeAwareDocStore.__init__(self,
                                   path_mapper=phylesystem_path_mapper,
                                   repos_dict=repos_dict,
                                   repos_par=repos_par,
                                   git_shard_class=PhylesystemShard,
                                   mirror_info=mirror_info,
                                   infrastructure_commit_author=infrastructure_commit_author,
                                   shard_mirror_pair_list=shard_mirror_pair_list)
        self._new_doc_prefix = self._growing_shard.new_doc_prefix  # TODO:shard-edits?
        self._growing_shard._determine_next_study_id()
        if with_caching:
            self._cache_region = _make_phylesystem_cache_region()
        else:
            self._cache_region = None
        self._cache_hits = 0

    def get_markdown_comment(self, document_obj):
        return document_obj.get('nexml', {}).get('^ot:comment', '')

    def get_study_ids(self):
        k = []
        for shard in self._shards:
            k.extend(shard.get_study_ids())
        return k

    # rename some generic members in the base class, for clarity and backward compatibility
    @property
    def return_study(self):
        return self.return_doc

    @property
    def get_changed_studies(self):
        return self.get_changed_docs

    @property
    def push_study_to_remote(self):
        return self.push_doc_to_remote

    @property
    def iter_study_objs(self):
        return self.iter_doc_objs

    @property
    def iter_study_filepaths(self):
        return self.iter_doc_filepaths

    @property
    def new_study_prefix(self):
        return self._new_doc_prefix

    @property
    def get_blob_sha_for_study_id(self):
        return self.get_blob_sha_for_doc_id

    @property
    def get_version_history_for_study_id(self):
        return self.get_version_history_for_doc_id

    @property
    def delete_study(self):
        return self.delete_doc

    @property
    def repo_nexml2json(self):
        return self.doc_schema.schema_version

    def _mint_new_study_id(self):
        """Checks out master branch of the shard as a side effect"""
        return self._growing_shard._mint_new_study_id()

    def ingest_new_study(self,
                         new_study_nexson,
                         auth_info,
                         new_study_id=None):
        placeholder_added = False
        if new_study_id is not None:
            raise NotImplementedError("Creating new studies with pre-assigned IDs was only supported when "
                                      "Open Tree of Life was still ingesting trees from phylografter.")
        try:
            gd, new_study_id = self.create_git_action_for_new_document(new_doc_id=new_study_id)
            try:
                nexml = new_study_nexson['nexml']
                nexml['^ot:studyId'] = new_study_id
                bundle = validate_and_convert_nexson(new_study_nexson,
                                                     self.doc_schema.schema_version,
                                                     allow_invalid=True)
                nexson, annotation, nexson_adaptor = bundle[0], bundle[1], bundle[3]
                r = self.annotate_and_write(doc_id=new_study_id,
                                            document=nexson,
                                            auth_info=auth_info,
                                            adaptor=nexson_adaptor,
                                            annotation=annotation,
                                            parent_sha=None,
                                            merged_sha=None,
                                            git_action=gd)
            except:
                self._growing_shard.delete_doc_from_index(new_study_id)
                raise
        except:
            if placeholder_added:
                with self._index_lock:
                    if new_study_id in self._doc2shard_map:
                        del self._doc2shard_map[new_study_id]
            raise
        with self._index_lock:
            self._doc2shard_map[new_study_id] = self._growing_shard
        return new_study_id, r

    # noinspection PyUnboundLocalVariable
    def add_validation_annotation(self, doc_obj, sha):
        need_to_cache = False
        adaptor = None
        annot_event = None
        if self._cache_region is not None:
            key = 'v' + sha
            annot_event = self._cache_region.get(key, ignore_expiration=True)
            if annot_event != NO_VALUE:
                _LOG.debug('cache hit for ' + key)
                adaptor = NexsonAnnotationAdder()
                self._cache_hits += 1
            else:
                _LOG.debug('cache miss for ' + key)
                need_to_cache = True
        if adaptor is None:
            bundle = ot_validate(doc_obj)
            annotation = bundle[0]
            annot_event = annotation['annotationEvent']
        replace_same_agent_annotation(doc_obj, annot_event)
        if need_to_cache:
            self._cache_region.set(key, annot_event)
            _LOG.debug('set cache for ' + key)
        return annot_event


_THE_PHYLESYSTEM = None


# noinspection PyPep8Naming
def Phylesystem(repos_dict=None,
                repos_par=None,
                mirror_info=None,
                infrastructure_commit_author='OpenTree API <api@opentreeoflife.org>',
                with_caching=True):
    """Factory function for a _Phylesystem object.

    A wrapper around the _Phylesystem class instantiation for
    the most common use case: a singleton _Phylesystem.
    If you need distinct _Phylesystem objects, you'll need to
    call that class directly.
    """
    global _THE_PHYLESYSTEM
    if _THE_PHYLESYSTEM is None:
        _THE_PHYLESYSTEM = _Phylesystem(repos_dict=repos_dict,
                                        repos_par=repos_par,
                                        with_caching=with_caching,
                                        mirror_info=mirror_info,
                                        infrastructure_commit_author=infrastructure_commit_author)
    return _THE_PHYLESYSTEM


def create_phylesystem_umbrella(shard_mirror_pair_list):
    return _Phylesystem(shard_mirror_pair_list=shard_mirror_pair_list)
