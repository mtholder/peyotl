# Simplified from peyotl.phylesystem.phylesystem_umbrella
#
# A collection id should be a unique "path" string composed
#     of '{owner_id}/{slugified-collection-name}'
#     EXAMPLES: 'jimallman/trees-about-bees'
#               'jimallman/other-interesting-stuff'
#               'kcranston/trees-about-bees'
#               'jimallman/trees-about-bees-2'
#
from peyotl.utility import get_logger
from peyotl.utility.str_util import slugify, increment_slug
import anyjson
from peyotl.git_storage import TypeAwareDocStore, ShardedDocStoreProxy
from peyotl.collections_store.collections_shard import (TreeCollectionsShard,
                                                        CollectionsFilepathMapper,
                                                        collections_path_mapper,
                                                        TreeCollectionsDocSchema)
from peyotl.collections_store.validation import validate_collection
import re

OWNER_ID_PATTERN = re.compile(r'^[a-zA-Z0-9-]+$')
# Allow simple slug-ified strings and slash separator (no whitespace!)

_LOG = get_logger(__name__)



class TreeCollectionStoreProxy(ShardedDocStoreProxy):
    """Proxy for shard when interacting with external resources if given the configuration of a remote Phylesystem
    """

    def __init__(self, config):
        ShardedDocStoreProxy.__init__(self, config, 'collections',
                                      path_mapper=collections_path_mapper,
                                      doc_schema=TreeCollectionsDocSchema)

class _TreeCollectionStore(TypeAwareDocStore):
    """Wrapper around a set of sharded git repos.
    """
    id_regex = CollectionsFilepathMapper.id_pattern
    def __init__(self,
                 repos_dict=None,
                 repos_par=None,
                 mirror_info=None,
                 infrastructure_commit_author='OpenTree API <api@opentreeoflife.org>',
                 **kwargs):
        """
        Repos can be found by passing in a `repos_par` (a directory that is the parent of the repos)
            or by trusting the `repos_dict` mapping of name to repo filepath.
        `with_caching` should be True for non-debugging uses.
        `git_action_class` is a subclass of GitActionBase to use. the __init__ syntax must be compatible
            with PhylesystemGitAction
        If you want to use a mirrors of the repo for pushes or pulls, send in a `mirror_info` dict:
            mirror_info['push'] and mirror_info['pull'] should be dicts with the following keys:
            'parent_dir' - the parent directory of the mirrored repos
            'remote_map' - a dictionary of remote name to prefix (the repo name + '.git' will be
                appended to create the URL for pushing).
        """
        TypeAwareDocStore.__init__(self,
                                   path_mapper=collections_path_mapper,
                                   repos_dict=repos_dict,
                                   repos_par=repos_par,
                                   git_shard_class=TreeCollectionsShard,
                                   mirror_info=mirror_info,
                                   infrastructure_commit_author='OpenTree API <api@opentreeoflife.org>',
                                   **kwargs)

    # rename some generic members in the base class, for clarity and backward compatibility
    @property
    def get_collection_ids(self):
        return self.get_doc_ids

    @property
    def delete_collection(self):
        return self.delete_doc


    def add_new_collection(self,
                           owner_id,
                           json_repr,
                           auth_info,
                           collection_id=None,
                           commit_msg=''):
        """Validate and save this JSON. Ensure (and return) a unique collection id"""
        collection = self._coerce_json_to_collection(json_repr)
        if collection is None:
            msg = "File failed to parse as JSON:\n{j}".format(j=json_repr)
            raise ValueError(msg)
        if not self._is_valid_document_json(collection):
            msg = "JSON is not a valid collection:\n{j}".format(j=json_repr)
            raise ValueError(msg)
        if collection_id:
            # try to use this id
            found_owner_id, slug = collection_id.split('/')
            assert found_owner_id == owner_id
        else:
            # extract a working title and "slugify" it
            slug = self._slugify_internal_collection_name(json_repr)
            collection_id = '{i}/{s}'.format(i=owner_id, s=slug)
        # Check the proposed id for uniqueness in any case. Increment until
        # we have a new id, then "reserve" it using a placeholder value.
        with self._index_lock:
            while collection_id in self._doc2shard_map:
                collection_id = increment_slug(collection_id)
            self._doc2shard_map[collection_id] = None
        # pass the id and collection JSON to a proper git action
        new_collection_id = None
        r = None
        try:
            # assign the new id to a shard (important prep for commit_and_try_merge2master)
            gd_id_pair = self.create_git_action_for_new_collection(new_collection_id=collection_id)
            new_collection_id = gd_id_pair[1]
            try:
                # let's remove the 'url' field; it will be restored when the doc is fetched (via API)
                del collection['url']
                # keep it simple (collection is already validated! no annotations needed!)
                r = self.commit_and_try_merge2master(file_content=collection,
                                                     doc_id=new_collection_id,
                                                     auth_info=auth_info,
                                                     parent_sha=None,
                                                     commit_msg=commit_msg,
                                                     merged_sha=None)
            except:
                self._growing_shard.delete_doc_from_index(new_collection_id)
                raise
        except:
            with self._index_lock:
                if new_collection_id in self._doc2shard_map:
                    del self._doc2shard_map[new_collection_id]
            raise
        with self._index_lock:
            self._doc2shard_map[new_collection_id] = self._growing_shard
        return new_collection_id, r

    def get_markdown_comment(self, document_obj):
        return document_obj.get('description', '')

    def copy_existing_collection(self, owner_id, old_collection_id):
        """Ensure a unique id, whether from the same user or a different one"""
        raise NotImplementedError('TODO')

    def rename_existing_collection(self, owner_id, old_collection_id, new_slug=None):
        """Use slug provided, or use internal name to generate a new id"""
        raise NotImplementedError('TODO')

    def _slugify_internal_collection_name(self, json_repr):
        """Parse the JSON, find its name, return a slug of its name"""
        collection = self._coerce_json_to_collection(json_repr)
        if collection is None:
            return None
        internal_name = collection['name']
        return slugify(internal_name)

_THE_TREE_COLLECTION_STORE = None


# noinspection PyPep8Naming
def TreeCollectionStore(repos_dict=None,
                        repos_par=None,
                        mirror_info=None,
                        infrastructure_commit_author='OpenTree API <api@opentreeoflife.org>'):
    """Factory function for a _TreeCollectionStore object.

    A wrapper around the _TreeCollectionStore class instantiation for
    the most common use case: a singleton _TreeCollectionStore.
    If you need distinct _TreeCollectionStore objects, you'll need to
    call that class directly.
    """
    global _THE_TREE_COLLECTION_STORE
    if _THE_TREE_COLLECTION_STORE is None:
        _THE_TREE_COLLECTION_STORE = _TreeCollectionStore(repos_dict=repos_dict,
                                                          repos_par=repos_par,
                                                          mirror_info=mirror_info,
                                                          infrastructure_commit_author=infrastructure_commit_author)
    return _THE_TREE_COLLECTION_STORE


def create_tree_collection_umbrella(shard_mirror_pair_list):
    return _TreeCollectionStore(shard_mirror_pair_list=shard_mirror_pair_list)
