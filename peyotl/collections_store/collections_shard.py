import os
from threading import Lock
from peyotl.utility import get_logger
from peyotl.collections_store.validation import validate_collection
from peyotl.git_storage.git_shard import TypeAwareGitShard
from peyotl.git_storage.type_aware_doc_store import SimpleJSONDocSchema
from peyotl.collections_store.git_actions import CollectionsFilepathMapper
_LOG = get_logger(__name__)

def filepath_for_collection_id(repo_dir, collection_id):
    # in this case, simply expand the id to a full path
    collection_filename = '{i}.json'.format(i=collection_id)
    full_path_to_file = os.path.join(repo_dir, 'collections-by-owner', collection_filename)
    _LOG.warn(">>>> filepath_for_collection_id: full path is {}".format(full_path_to_file))
    return full_path_to_file


class TreeCollectionsDocSchema(SimpleJSONDocSchema):
    def __init__(self):
        SimpleJSONDocSchema.__init__(self, document_type='tree collection JSON')

    def __repr__(self):
        return 'TreeCollectionsDocSchema()'

    def create_empty_doc(self):
        collection = {
            "url": "",
            "name": "",
            "description": "",
            "creator": {"login": "", "name": ""},
            "contributors": [],
            "decisions": [],
            "queries": []
        }
        return collection

    def validate_annotate_convert_doc(self, document, **kwargs):
        """No conversion between different schema is supported for collections"""
        errors, adaptor = validate_collection(document)
        return document, errors, None, adaptor


def create_id2collection_info(path, tag):
    """Searchers for JSON files in this repo and returns
    a map of collection id ==> (`tag`, dir, collection filepath)
    where `tag` is typically the shard name
    """
    d = {}
    for triple in os.walk(path):
        root, files = triple[0], triple[2]
        for filename in files:
            if filename.endswith('.json'):
                # trim file extension and prepend owner_id (from path)
                collection_id = "{u}/{n}".format(u=root.split('/')[-1], n=filename[:-5])
                d[collection_id] = (tag, root, os.path.join(root, filename))
    return d


def refresh_collection_index(shard, initializing=False):
    d = create_id2collection_info(shard.doc_dir, shard.name)
    shard._doc_index = d


class TreeCollectionsShard(TypeAwareGitShard):
    """Wrapper around a git repo holding JSON tree collections
    Raises a ValueError if the directory does not appear to be a TreeCollectionsShard.
    Raises a RuntimeError for errors associated with misconfiguration."""
    from peyotl.phylesystem.git_actions import PhylesystemGitAction
    document_type = 'tree_collection'

    def __init__(self,
                 name,
                 path,
                 git_action_class=PhylesystemGitAction,
                 push_mirror_repo_path=None,
                 infrastructure_commit_author='OpenTree API <api@opentreeoflife.org>'):
        TypeAwareGitShard.__init__(self,
                                   name=name,
                                   path=path,
                                   doc_holder_subpath='collections-by-owner',
                                   doc_schema=TreeCollectionsDocSchema(),
                                   refresh_doc_index_fn=refresh_collection_index,  # populates _doc_index
                                   git_action_class=git_action_class,
                                   push_mirror_repo_path=push_mirror_repo_path,
                                   infrastructure_commit_author=infrastructure_commit_author)

    # rename some generic members in the base class, for clarity and backward compatibility
    @property
    def known_prefixes(self):
        if self._known_prefixes is None:
            self._known_prefixes = self._diagnose_prefixes()
        return self._known_prefixes

    def _diagnose_prefixes(self):
        """Returns a set of all of the prefixes seen in the main document dir
        """
        p = set()
        for owner_dirname in os.listdir(self.doc_dir):
            example_collection_name = "{n}/xxxxx".format(n=owner_dirname)
            if CollectionsFilepathMapper.id_pattern.match(example_collection_name):
                p.add(owner_dirname)
        return p

    def create_git_action_for_new_collection(self, new_collection_id=None):
        """Checks out master branch as a side effect"""
        ga = self.create_git_action()
        assert new_collection_id is not None
        # id should have been sorted out by the caller
        self.register_doc_id(ga, new_collection_id)
        return ga, new_collection_id

    def _create_git_action_for_global_resource(self):
        return self._ga_class(repo=self.path,
                              max_file_size=self.max_file_size)
