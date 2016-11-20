import os
import re
from peyotl.utility import get_logger
from peyotl.collections_store.validation.adaptor import CollectionValidationAdaptor
from peyotl.git_storage.git_shard import TypeAwareGitShard
from peyotl.git_storage.type_aware_doc_store import SimpleJSONDocSchema
from peyotl.git_storage import GitActionBase
_LOG = get_logger(__name__)


class CollectionsFilepathMapper(object):
    id_pattern =  re.compile(r'^[a-zA-Z0-9-]+/[a-z0-9-]+$')
    wip_id_template = r'.*_collection_{i}_[0-9]+',
    branch_name_template = "{ghu}_collection_{rid}",
    path_to_user_splitter = '_collection_'
    doc_holder_subpath = 'collections-by-owner'
    doc_parent_dir = 'collections-by-owner/'
    def filepath_for_id(self, repo_dir, collection_id):
        assert bool(CollectionsFilepathMapper.id_pattern.match(collection_id))
        return '{r}/collections-by-owner/{s}.json'.format(r=repo_dir, s=collection_id)

    def id_from_rel_path(self, path):
        doc_parent_dir = 'collections-by-owner/'
        if path.startswith(doc_parent_dir):
            p = path.split(doc_parent_dir)[1]
            if p.endswith('.json'):
                return p[:-5]
            return p

collections_path_mapper = CollectionsFilepathMapper()

def filepath_for_collection_id(repo_dir, collection_id):
    # in this case, simply expand the id to a full path
    collection_filename = '{i}.json'.format(i=collection_id)
    full_path_to_file = os.path.join(repo_dir, 'collections-by-owner', collection_filename)
    _LOG.warn(">>>> filepath_for_collection_id: full path is {}".format(full_path_to_file))
    return full_path_to_file


class TreeCollectionsDocSchema(SimpleJSONDocSchema):
    def __init__(self):
        SimpleJSONDocSchema.__init__(self,
                                     document_type='tree collection JSON',
                                     adaptor_factory=CollectionValidationAdaptor)

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



class TreeCollectionsShard(TypeAwareGitShard):
    """Wrapper around a git repo holding JSON tree collections
    Raises a ValueError if the directory does not appear to be a TreeCollectionsShard.
    Raises a RuntimeError for errors associated with misconfiguration."""
    def __init__(self,
                 name,
                 path,
                 push_mirror_repo_path=None,
                 infrastructure_commit_author='OpenTree API <api@opentreeoflife.org>'):
        TypeAwareGitShard.__init__(self,
                                   name=name,
                                   path=path,
                                   doc_schema=TreeCollectionsDocSchema(),
                                   push_mirror_repo_path=push_mirror_repo_path,
                                   infrastructure_commit_author=infrastructure_commit_author,
                                   path_mapper=collections_path_mapper)

    def _diagnose_prefixes(self):
        """Returns a set of all of the prefixes seen in the main document dir
        """
        p = set()
        for owner_dirname in os.listdir(self.doc_dir):
            example_collection_name = "{n}/xxxxx".format(n=owner_dirname)
            if CollectionsFilepathMapper.id_pattern.match(example_collection_name):
                p.add(owner_dirname)
        return p
