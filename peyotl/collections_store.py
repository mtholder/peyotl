# !/usr/bin/env python
"""
"""
import os
import re
from peyotl.validation import validate_dict_keys
from peyotl.git_storage.git_shard import TypeAwareGitShard
from peyotl.git_storage.type_aware_doc_store import SimpleJSONDocSchema
from peyotl.utility.input_output import read_as_json
from peyotl.utility.str_util import is_str_type, string_types_tuple
from peyotl.utility import get_logger
from peyotl.utility.str_util import slugify, increment_slug
from peyotl.git_storage import (GitShardFilepathMapper, TypeAwareDocStore, ShardedDocStoreProxy,
                                NonAnnotatingDocValidationAdaptor)
from peyotl.validation import SimpleCuratorSchema

_LOG = get_logger(__name__)


###############################################################################
# ID <-> Filepath logic
# noinspection PyMethodMayBeStatic
class CollectionsFilepathMapper(GitShardFilepathMapper):
    id_pattern = re.compile(r'^[a-zA-Z0-9-]+/[a-z0-9-]+$')

    def __init__(self):
        GitShardFilepathMapper.__init__(self, 'collection',
                                        doc_holder_subpath_list=('collections-by-owner',))

    def prefix_from_doc_id(self, doc_id):
        # The collection id is a sort of "path", e.g. '{owner_id}/{collection-name-as-slug}'
        #   EXAMPLES: 'jimallman/trees-about-bees', 'kcranston/interesting-trees-2'
        # Assume that the owner_id will work as a prefix, esp. by assigning all of a
        # user's collections to a single shard.for grouping in shards
        _LOG.debug('> prefix_from_collection_path(), testing this id: {i}'.format(i=doc_id))
        path_parts = doc_id.split('/')
        _LOG.debug('> prefix_from_collection_path(), found {} path parts'.format(len(path_parts)))
        if len(path_parts) > 1:
            owner_id = path_parts[0]
        elif path_parts[0] == '':
            owner_id = 'anonymous'
        else:
            owner_id = 'anonymous'  # or perhaps None?
        return owner_id


collections_path_mapper = CollectionsFilepathMapper()

# End ID <-> Filepath logid
###############################################################################
# Tree Collections Schema
_string_types = string_types_tuple()


class _TreeCollectionTopLevelSchema(object):
    required_elements = {
        # N.B. anyjson might parse a text element as str or unicode,
        # depending on its value. Either is fine here.
        'url': _string_types,
        'name': _string_types,
        'description': _string_types,
        'creator': dict,
        'contributors': list,
        'decisions': list,
        'queries': list,
    }
    optional_elements = {}
    allowed_elements = frozenset(required_elements.keys())


# TODO: Define a simple adapter based on
# nexson_validation._badgerfish_validation.BadgerFishValidationAdapter.
# N.B. that this doesn't need to inherit from NexsonValidationAdapter, since
# we're not adding annotations to the target document. Similarly, we're not using
# the usual validation logger here, just a list of possible error strings.
class CollectionValidationAdaptor(NonAnnotatingDocValidationAdaptor):
    def __init__(self, obj, errors, **kwargs):
        validate_dict_keys(obj, _TreeCollectionTopLevelSchema, errors, 'collection')
        # test a non-empty creator for expected 'login' and 'name' fields
        self._creator = obj.get('creator')
        if isinstance(self._creator, dict):
            validate_dict_keys(self._creator, SimpleCuratorSchema, errors, 'collection.creator')
        # test any contributors for expected 'login' and 'name' fields
        self._contributors = obj.get('contributors')
        if isinstance(self._contributors, list):
            for c in self._contributors:
                if isinstance(c, dict):
                    validate_dict_keys(c, SimpleCuratorSchema, errors, 'collection.contributors element')
                else:
                    errors.append("Unexpected type for contributor (should be dict)")
        # test decisions for valid ids+SHA, valid decision value
        # N.B. that we use the list position for implicit ranking and
        # disregard this position for EXCLUDED trees.
        self._decisions = obj.get('decisions')
        if isinstance(self._decisions, list):
            text_props = ['name', 'studyID', 'treeID', 'SHA', 'decision']
            decision_values = ['INCLUDED', 'EXCLUDED', 'UNDECIDED']
            for d in self._decisions:
                try:
                    assert d.get('decision') in decision_values
                except:
                    errors.append("Each 'decision' should be one of {dl}".format(dl=decision_values))
                for p in text_props:
                    try:
                        assert isinstance(d.get(p), _string_types)
                    except:
                        errors.append("Decision property '{p}' should be one of {t}".format(p=p, t=_string_types))
        # TODO: test queries (currently unused) for valid properties
        self._queries = obj.get('queries')


def validate_collection(obj, **kwargs):
    """Takes an `obj` that is a collection object.
    Returns the pair:
        errors, adaptor
    `errors` is a simple list of error messages
    `adaptor` will be an instance of collections.validation.adaptor.CollectionValidationAdaptor
        it holds a reference to `obj` and the bookkeepping data necessary to attach
        the log message to `obj` if
    """
    # Gather and report errors in a simple list
    errors = []
    n = CollectionValidationAdaptor(obj, errors, **kwargs)
    return errors, n


def collection_to_included_trees(collection):
    """Takes a collection object (or a filepath to collection object), returns
    each element of the `decisions` list that has the decision set to included.
    """
    if is_str_type(collection):
        collection = read_as_json(collection)
    inc = []
    for d in collection.get('decisions', []):
        if d['decision'] == 'INCLUDED':
            inc.append(d)
    return inc


def tree_is_in_collection(collection, study_id=None, tree_id=None):
    """Takes a collection object (or a filepath to collection object), returns
    True if it includes a decision to include the specified tree
    """
    included = collection_to_included_trees(collection)
    study_id = study_id.strip()
    tree_id = tree_id.strip()
    for decision in included:
        if decision['studyID'] == study_id and decision['treeID'] == tree_id:
            return True
    return False


def concatenate_collections(collection_list):
    r = TreeCollectionsDocSchema().create_empty_doc()
    r_decisions = r['decisions']
    r_contributors = r['contributors']
    r_queries = r['queries']
    contrib_set = set()
    inc_set = set()
    not_inc_set = set()
    for n, coll in enumerate(collection_list):
        r_queries.extend(coll['queries'])
        for contrib in coll['contributors']:
            l = contrib['login']
            if l not in contrib_set:
                r_contributors.append(contrib)
                contrib_set.add(l)
        for d in coll['decisions']:
            key = '{}_{}'.format(d['studyID'], d['treeID'])
            inc_d = d['decision'].upper() == 'INCLUDED'
            if key in inc_set:
                if not inc_d:
                    raise ValueError('Collections disagree on inclusion of study_tree = "{}"'.format(key))
            elif key in not_inc_set:
                if inc_d:
                    raise ValueError('Collections disagree on inclusion of study_tree = "{}"'.format(key))
            else:
                if inc_d:
                    inc_set.add(key)
                else:
                    not_inc_set.add(key)
                r_decisions.append(d)
    return r


class TreeCollectionsDocSchema(SimpleJSONDocSchema):
    def __init__(self):
        SimpleJSONDocSchema.__init__(self,
                                     document_type='tree collection JSON',
                                     adaptor_factory=CollectionValidationAdaptor)

    def __repr__(self):
        return 'TreeCollectionsDocSchema()'

    # noinspection PyMethodMayBeStatic
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
                                   document_schema=TreeCollectionsDocSchema(),
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


OWNER_ID_PATTERN = re.compile(r'^[a-zA-Z0-9-]+$')


class TreeCollectionStoreProxy(ShardedDocStoreProxy):
    """Proxy for shard when interacting with external resources if given the configuration of a remote Phylesystem
    """

    def __init__(self, config):
        ShardedDocStoreProxy.__init__(self, config, 'collections',
                                      path_mapper=collections_path_mapper,
                                      document_schema=TreeCollectionsDocSchema)


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
                                   infrastructure_commit_author=infrastructure_commit_author,
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
        if collection_id:
            # try to use this id
            found_owner_id, slug = collection_id.split('/')
            assert found_owner_id == owner_id
        else:
            # extract a working title and "slugify" it
            slug = self._slugify_internal_collection_name(json_repr)
            collection_id = '{i}/{s}'.format(i=owner_id, s=slug)
        return self.add_new_doc(json_repr,
                                auth_info=auth_info,
                                doc_id=collection_id,
                                commit_msg=commit_msg)

    def add_new_doc(self, json_repr, auth_info, commit_msg='', doc_id=None):
        """Validate and save this JSON. Ensure (and return) a unique collection id"""
        collection = self._coerce_json_to_document(json_repr)
        if collection is None:
            msg = "File failed to parse as JSON:\n{j}".format(j=json_repr)
            raise ValueError(msg)
        if not self._is_valid_document_json(collection):
            msg = "JSON is not a valid collection:\n{j}".format(j=json_repr)
            raise ValueError(msg)
        owner_id = auth_info['login']
        if doc_id:
            # try to use this id
            found_owner_id, slug = doc_id.split('/')
            assert found_owner_id == owner_id
        else:
            # extract a working title and "slugify" it
            slug = self._slugify_internal_collection_name(json_repr)
            doc_id = '{i}/{s}'.format(i=owner_id, s=slug)
        # Check the proposed id for uniqueness in any case. Increment until
        # we have a new id, then "reserve" it using a placeholder value.
        with self._index_lock:
            while doc_id in self._doc2shard_map:
                doc_id = increment_slug(doc_id)
            self._doc2shard_map[doc_id] = None
        # pass the id and collection JSON to a proper git action
        new_doc_id = None
        try:
            # assign the new id to a shard (important prep for commit_and_try_merge2master)
            gd_id_pair = self.create_git_action_for_new_document(new_doc_id=doc_id)
            new_doc_id = gd_id_pair[1]
            try:
                # let's remove the 'url' field; it will be restored when the doc is fetched (via API)
                del collection['url']
                # keep it simple (collection is already validated! no annotations needed!)
                r = self.commit_and_try_merge2master(file_content=collection,
                                                     doc_id=new_doc_id,
                                                     auth_info=auth_info,
                                                     parent_sha=None,
                                                     commit_msg=commit_msg,
                                                     merged_sha=None)
            except:
                self._growing_shard.delete_doc_from_index(new_doc_id)
                raise
        except:
            with self._index_lock:
                if new_doc_id in self._doc2shard_map:
                    del self._doc2shard_map[new_doc_id]
            raise
        with self._index_lock:
            self._doc2shard_map[new_doc_id] = self._growing_shard
        return new_doc_id, r

    def append_include_decision(self, doc_id, include_decision, auth_info, commit_msg):
        """Appends `include_decision` to the collection doc_id"""
        collection, parent_sha = self.return_doc(doc_id)
        collection['decisions'].append(include_decision)
        return self.commit_and_try_merge2master(file_content=collection,
                                                doc_id=doc_id,
                                                auth_info=auth_info,
                                                parent_sha=parent_sha,
                                                commit_msg=commit_msg,
                                                merged_sha=None)

    def purge_tree_from_collection(self, doc_id, study_id, tree_id, auth_info, commit_msg):
        """Removes any decision involving (study_id, tree_id) from the collection doc_id"""
        collection, parent_sha = self.return_doc(doc_id)
        decision_list = collection['decisions']
        nd = [d for d in decision_list if not (d['studyID'] == study_id and d['treeID'] == tree_id)]
        if len(nd) == len(decision_list):
            return {}
        collection['decisions'] = nd
        return self.commit_and_try_merge2master(file_content=collection,
                                                doc_id=doc_id,
                                                auth_info=auth_info,
                                                parent_sha=parent_sha,
                                                commit_msg=commit_msg,
                                                merged_sha=None)

    @staticmethod
    def collection_includes_tree(collection, study_id, tree_id):
        for decision in collection.get('decisions', []):
            if decision['studyID'] == study_id and decision['treeID'] == tree_id:
                return True
        return False

    @staticmethod
    def create_tree_inclusion_decision(study_id, tree_id, name='', comment='', sha=''):
        return {'name': name,
                'treeID': tree_id,
                'studyID': study_id,
                'SHA': sha,
                'decision': "INCLUDED",
                'comments': comment}

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
        collection = self._coerce_json_to_document(json_repr)
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
