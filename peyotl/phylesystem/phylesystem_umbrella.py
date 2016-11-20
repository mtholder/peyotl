from peyotl.utility import get_logger, get_config_setting

try:
    # noinspection PyPackageRequirements
    from dogpile.cache.api import NO_VALUE
except:
    pass  # caching is optional
from peyotl.phylesystem.helper import _make_phylesystem_cache_region
from peyotl.git_storage import (ShardedDocStore, ShardedDocStoreProxy, TypeAwareDocStore)
from peyotl.phylesystem.phylesystem_shard import PhylesystemShardProxy, PhylesystemShard
from peyotl.phylesystem.git_workflows import validate_and_convert_nexson
from peyotl.nexson_validation import ot_validate
from peyotl.nexson_validation._validation_base import NexsonAnnotationAdder, replace_same_agent_annotation
from peyotl.phylesystem.git_actions import PhylesystemFilepathMapper

_LOG = get_logger(__name__)


def prefix_from_study_id(study_id):
    # TODO: Use something smarter here, splitting on underscore?
    return study_id[:3]


class PhylesystemProxy(ShardedDocStoreProxy):
    """Proxy for interacting with external resources if given the configuration of a remote Phylesystem.
    N.B. that this has minimal functionality, and is mainly used to fetch studies and their ids.
    """

    def __init__(self, config):
        ShardedDocStore.__init__(self,
                                 prefix_from_doc_id=prefix_from_study_id)
        self._shards = []
        for s in config.get('shards', []):
            self._shards.append(PhylesystemShardProxy(s))
        self.create_doc_index('study')


class _Phylesystem(TypeAwareDocStore):
    """Wrapper around a set of sharded git repos, with business rules specific to Nexson studies.
    """
    id_regex = PhylesystemFilepathMapper.id_pattern
    document_type = 'study'

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
                                   prefix_from_doc_id=prefix_from_study_id,
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
        return document_obj, get('nexml', {}).get('^ot:comment', '')

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

    def create_git_action_for_new_study(self, new_study_id=None):
        """Checks out master branch of the shard as a side effect"""
        return self._growing_shard.create_git_action_for_new_study(new_study_id=new_study_id)

    def ingest_new_study(self,
                         new_study_nexson,
                         auth_info,
                         new_study_id=None):
        placeholder_added = False
        if new_study_id is not None:
            raise NotImplementedError("Creating new studies with pre-assigned IDs was only supported when "
                                      "Open Tree of Life was still ingesting trees from phylografter.")
        try:
            gd, new_study_id = self.create_git_action_for_new_study(new_study_id=new_study_id)
            try:
                nexml = new_study_nexson['nexml']
                nexml['^ot:studyId'] = new_study_id
                bundle = validate_and_convert_nexson(new_study_nexson,
                                                     self.doc_schema.schema_version,
                                                     allow_invalid=True)
                nexson, annotation, nexson_adaptor = bundle[0], bundle[1], bundle[3]
                r = self.annotate_and_write(git_data=gd,
                                            nexson=nexson,
                                            doc_id=new_study_id,
                                            auth_info=auth_info,
                                            adaptor=nexson_adaptor,
                                            annotation=annotation,
                                            parent_sha=None,
                                            master_file_blob_included=None)
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
            # del annot_event['@dateCreated'] #TEMP
            # del annot_event['@id'] #TEMP
            adaptor = bundle[2]
        replace_same_agent_annotation(doc_obj, annot_event)
        if need_to_cache:
            self._cache_region.set(key, annot_event)
            _LOG.debug('set cache for ' + key)
        return annot_event


_THE_PHYLESYSTEM = None


def Phylesystem(repos_dict=None,
                repos_par=None,
                mirror_info=None,
                new_study_prefix=None,  # Unused, TEMP deprecated
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
