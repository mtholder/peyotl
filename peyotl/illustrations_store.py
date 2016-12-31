# !/usr/bin/env python
"""Basic functions for creating and manipulating amendments JSON.
"""
import os
import re

import dateutil.parser
from peyotl.validation import validate_dict_keys
from peyotl.validation import SimpleCuratorSchema as _AmendmentCuratorSchema
from peyotl import (get_logger, doi2url, string_types_tuple, slugify)
from peyotl.git_storage import (GitShardFilepathMapper, ShardedDocStoreProxy, TypeAwareDocStore,
                                NonAnnotatingDocValidationAdaptor)
from peyotl.git_storage.git_shard import TypeAwareGitShard
from peyotl.git_storage.type_aware_doc_store import SimpleJSONDocSchema
from peyotl.phylesystem import PhylesystemFilepathMapper

_LOG = get_logger(__name__)


###############################################################################
# ID <-> Filepath logic
# noinspection PyMethodMayBeStatic,PyMethodMayBeStatic
class IllustrationStoreFilepathMapper(GitShardFilepathMapper):
    id_pattern = re.compile(r'^TODO-[0-9]+$')

    def __init__(self):
        dl =  (primary_doc_tag, 'templates', 'style-guides')
        GitShardFilepathMapper.__init__(self, 'docs-by-owner', doc_holder_subpath_list=dl)

    def filepath_for_id(self, repo_dir, doc_id):
        assert bool(IllustrationStoreFilepathMapper.id_pattern.match(doc_id))
        fn = '{s}.json'.format(s=doc_id)
        return os.path.join(repo_dir, 'illustration', fn)

# immutable, singleton "FilepathMapper" objects are passed to the GitAction
#   initialization function as a means of making the mapping of a document ID
#   to the filepath generic across document type.
illustration_path_mapper = IllustrationStoreFilepathMapper()

# End ID <-> Filepath logid
###############################################################################
# Illustrations Schemas
_string_types = string_types_tuple()


class _IllustrationTopLevelSchema(object):
    required_elements = {
        # N.B. anyjson might parse a text element as str or unicode,
        # depending on its value. Either is fine here.
        'date_created': _string_types,
    }
    optional_elements = {
        'id': _string_types,  # not present in initial request
    }
    allowed_elements = set(required_elements.keys())
    allowed_elements.update(optional_elements.keys())
    allowed_elements = frozenset(allowed_elements)



class IllustrationValidationAdaptor(NonAnnotatingDocValidationAdaptor):
    # noinspection PyUnusedLocal
    def __init__(self, obj, errors, **kwargs):
        validate_dict_keys(obj, _IllustrationTopLevelSchema, errors, 'illustration')
        # test a non-empty id against our expected pattern
        self._id = obj.get('id')
        if self._id and not (isinstance(self._id, _string_types)
                             and bool(IllustrationStoreFilepathMapper.id_pattern.match(self._id))):
            errors.append("The top-level illustration 'id' provided is not valid")
        self._date_created = obj.get('date_created')
        try:
            dateutil.parser.parse(self._date_created)
        except:
            errors.append("Property 'date_created' is not a valid ISO date")

# noinspection PyMethodMayBeStatic
class IllustationDocSchema(SimpleJSONDocSchema):
    def __init__(self):
        SimpleJSONDocSchema.__init__(self,
                                     document_type='illustration JSON',
                                     adaptor_factory=IllustrationValidationAdaptor)

    def __repr__(self):
        return 'IllustationDocSchema()'

    def create_empty_doc(self):
        import datetime
        illustration = {
            "id": "",  # assigned when new ottids are minted
            "date_created": datetime.datetime.utcnow().date().isoformat(),
        }
        return illustration


def validate_illustration(obj):
    """returns a list of errors and a AmendmentValidationAdaptor object for `obj`"""
    return IllustationDocSchema().validate(obj)


# End Validation
###############################################################################
# Shard

# noinspection PyMethodMayBeStatic
class IllustrationsShard(TypeAwareGitShard):
    """Wrapper around a git repo holding JSON taxonomic amendments
    Raises a ValueError if the directory does not appear to be a TaxonomicAmendmentsShard.
    Raises a RuntimeError for errors associated with misconfiguration."""

    def __init__(self,
                 name,
                 path,
                 push_mirror_repo_path=None,
                 infrastructure_commit_author='OpenTree API <api@opentreeoflife.org>'):
        TypeAwareGitShard.__init__(self,
                                   name=name,
                                   path=path,
                                   document_schema=IllustationDocSchema(),
                                   push_mirror_repo_path=push_mirror_repo_path,
                                   infrastructure_commit_author=infrastructure_commit_author,
                                   path_mapper=illustration_path_mapper)

class IllustrationStoreProxy(ShardedDocStoreProxy):
    """Proxy for interacting with external resources if given the configuration of a remote TaxonomicAmendmentStore
    """

    def __init__(self, config):
        ShardedDocStoreProxy.__init__(self, config, 'illustrations',
                                      path_mapper=illustration_path_mapper,
                                      document_schema=IllustationDocSchema)


# noinspection PyProtectedMember
class _IllustrationStore(TypeAwareDocStore):
    """Wrapper around a set of sharded git repos.
    """
    id_regex = IllustrationStoreFilepathMapper.id_pattern

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
        If you want to use a mirrors of the repo for pushes or pulls, send in a `mirror_info` dict:
            mirror_info['push'] and mirror_info['pull'] should be dicts with the following keys:
            'parent_dir' - the parent directory of the mirrored repos
            'remote_map' - a dictionary of remote name to prefix (the repo name + '.git' will be
                appended to create the URL for pushing).
        """
        TypeAwareDocStore.__init__(self,
                                   path_mapper=illustration_path_mapper,
                                   repos_dict=repos_dict,
                                   repos_par=repos_par,
                                   git_shard_class=IllustrationsShard,
                                   mirror_info=mirror_info,
                                   infrastructure_commit_author=infrastructure_commit_author,
                                   **kwargs)

    def add_new_doc(self, json_repr, auth_info, commit_msg='', doc_id=None):
        """Validate and save this JSON. Ensure (and return) a unique amendment id"""
        if doc_id is not None:
            nim = "Creating new illustrations with pre-assigned IDs is not supported."
            raise NotImplementedError(nim)
        illustration = self._coerce_json_to_document(json_repr)
        if illustration is None:
            msg = "File failed to parse as JSON:\n{j}".format(j=json_repr)
            raise ValueError(msg)
        if not self._is_valid_document_json(illustration):
            msg = "JSON is not a valid illustration:\n{j}".format(j=json_repr)
            raise ValueError(msg)
        doc_id = "TODO"
        return self.add_new_doc(illustration,
                                auth_info=auth_info,
                                doc_id=doc_id,
                                commit_msg=commit_msg)

    add_new_illustration = add_new_doc


_ILLUSTRATION_STORE = None


# noinspection PyPep8Naming
def IllustrationStore(repos_dict=None,
                            repos_par=None,
                            mirror_info=None,
                            infrastructure_commit_author='OpenTree API <api@opentreeoflife.org>'):
    """Factory function for a _TaxonomicAmendmentStore object.

    A wrapper around the _TaxonomicAmendmentStore class instantiation for
    the most common use case: a singleton _TaxonomicAmendmentStore.
    If you need distinct _TaxonomicAmendmentStore objects, you'll need to
    call that class directly.
    """
    global _ILLUSTRATION_STORE
    if _ILLUSTRATION_STORE is None:
        r = _IllustrationStore(repos_dict=repos_dict,
                                     repos_par=repos_par,
                                     mirror_info=mirror_info,
                                     infrastructure_commit_author=infrastructure_commit_author)
        _ILLUSTRATION_STORE = r
    return _ILLUSTRATION_STORE


def create_taxonomic_amendments_umbrella(shard_mirror_pair_list):
    return _IllustrationStore(shard_mirror_pair_list=shard_mirror_pair_list)
