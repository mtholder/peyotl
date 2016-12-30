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
        dhspl =  (primary_doc_tag, 'templates', 'style-guides')
        GitShardFilepathMapper.__init__(self, 'illustrations', doc_holder_subpath_list=dhspl)

    def filepath_for_id(self, repo_dir, doc_id):
        assert bool(AmendmentFilepathMapper.id_pattern.match(doc_id))
        fn = '{s}.json'.format(s=doc_id)
        return os.path.join(repo_dir, 'illustration', fn)

# immutable, singleton "FilepathMapper" objects are passed to the GitAction
#   initialization function as a means of making the mapping of a document ID
#   to the filepath generic across document type.
illustration_path_mapper = IllustrationStoreFilepathMapper()

# End ID <-> Filepath logid
###############################################################################
'''
# Amendment Schema
_string_types = string_types_tuple()


class _AmendmentTopLevelSchema(object):
    required_elements = {
        # N.B. anyjson might parse a text element as str or unicode,
        # depending on its value. Either is fine here.
        'curator': dict,
        'date_created': _string_types,
        'taxa': list,
        'user_agent': _string_types,
    }
    optional_elements = {
        'id': _string_types,  # not present in initial request
        'study_id': _string_types,
        'new_ottids_required': int,  # provided by some agents
    }
    allowed_elements = set(required_elements.keys())
    allowed_elements.update(optional_elements.keys())
    allowed_elements = frozenset(allowed_elements)


class _AmendmentTaxonSchema(object):
    required_elements = {
        'name': _string_types,
        'name_derivation': _string_types,  # from controlled vocabulary
        'sources': list,
    }
    optional_elements = {
        'comment': _string_types,
        'rank': _string_types,  # can be 'no rank'
        'original_label': _string_types,
        'adjusted_label': _string_types,
        'parent': int,  # the parent taxon's OTT id
        'parent_tag': _string_types,
        'tag': object,  # can be anything (int, string, ...)
        'ott_id': int  # if already assigned
    }
    allowed_elements = set(required_elements.keys())
    allowed_elements.update(['parent', 'parent_tag'])
    allowed_elements.update(optional_elements.keys())
    allowed_elements = frozenset(allowed_elements)


class _AmendmentSourceSchema(object):
    # we need at least one source with type and (sometimes) non-empty value
    requiring_value = frozenset([
        'Link to online taxonomy',
        'Link (DOI) to publication',
        'Other',
    ])
    not_requiring_value = frozenset(['The taxon is described in this study', ])
    allowed_elements = set(requiring_value)
    allowed_elements.update(not_requiring_value)
    allowed_elements = frozenset(allowed_elements)
    requiring_URL = frozenset(['Link to online taxonomy', 'Link (DOI) to publication', ])


def _validate_amendment_source(s, errors):
    valid_source_found = False
    s_type = s.get('source_type', None)
    if s_type not in _AmendmentSourceSchema.allowed_elements:
        errors.append("Unknown taxon source type '{t}'!".format(t=s_type))
    elif s_type in _AmendmentSourceSchema.requiring_value:
        if s.get('source'):
            valid_source_found = True
        else:
            errors.append("Missing value for taxon source of type '{t}'!".format(t=s_type))
    else:
        valid_source_found = True
    if s_type in _AmendmentSourceSchema.requiring_URL:
        s_val = s.get('source')
        if not (s_val and s_val == doi2url(s_val)):
            msg = "Source '{s}' (of type '{t}') should be a URL!".format(s=s_val, t=s_type)
            errors.append(msg)
    return valid_source_found


class AmendmentValidationAdaptor(NonAnnotatingDocValidationAdaptor):
    # noinspection PyUnusedLocal
    def __init__(self, obj, errors, **kwargs):
        validate_dict_keys(obj, _AmendmentTopLevelSchema, errors, 'amendment')
        # test a non-empty id against our expected pattern
        self._id = obj.get('id')
        if self._id and not (isinstance(self._id, _string_types)
                             and bool(AmendmentFilepathMapper.id_pattern.match(self._id))):
            errors.append("The top-level amendment 'id' provided is not valid")
        # test a non-empty curator for expected 'login' and 'name' fields
        self._curator = obj.get('curator')
        if isinstance(self._curator, dict):
            validate_dict_keys(self._curator, _AmendmentCuratorSchema, errors, 'amendment.curator')
        # date_created should be valid ISO 8601
        self._date_created = obj.get('date_created')
        try:
            dateutil.parser.parse(self._date_created)
        except:
            errors.append("Property 'date_created' is not a valid ISO date")
        # study_id (if it's not an empty string)
        self._study_id = obj.get('study_id')
        if self._study_id and isinstance(self._study_id, _string_types):
            if not bool(PhylesystemFilepathMapper.id_pattern.match(self._study_id)):
                errors.append("The 'study_id' provided is not valid")
        self._taxa = obj.get('taxa')
        if isinstance(self._taxa, list):
            for taxon in self._taxa:
                if not isinstance(taxon, dict):
                    errors.append('Expecting each element of amendment.taxa to be a dict')
                else:
                    validate_dict_keys(taxon, _AmendmentTaxonSchema, errors, 'amendment.taxa element')
                    if ('parent' not in taxon) and ('parent_tag' not in taxon):
                        errors.append("Taxon has neither 'parent' nor 'parent_tag'!")
                num_valid_sources = 0
                for s in taxon.get('sources', []):
                    num_valid_sources += 1 if _validate_amendment_source(s, errors) else 0
                if num_valid_sources == 0:
                    errors.append("Taxon must have at least one valid source (none found)!")
        else:
            errors.append('Expecting amendment.taxa to be a list')


# noinspection PyMethodMayBeStatic
class TaxonomicAmendmentDocSchema(SimpleJSONDocSchema):
    def __init__(self):
        SimpleJSONDocSchema.__init__(self,
                                     document_type='taxon amendment JSON',
                                     adaptor_factory=AmendmentValidationAdaptor)

    def __repr__(self):
        return 'TaxonomicAmendmentDocSchema()'

    def create_empty_doc(self):
        import datetime
        amendment = {
            "id": "",  # assigned when new ottids are minted
            "curator": {"login": "", "name": ""},
            "date_created": datetime.datetime.utcnow().date().isoformat(),
            "user_agent": "",
            "study_id": "",
            "taxa": [],
        }
        return amendment


def validate_amendment(obj):
    """returns a list of errors and a AmendmentValidationAdaptor object for `obj`"""
    return TaxonomicAmendmentDocSchema().validate(obj)


# End Validation
###############################################################################
# Shard

# noinspection PyMethodMayBeStatic
class TaxonomicAmendmentsShard(TypeAwareGitShard):
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
                                   document_schema=TaxonomicAmendmentDocSchema(),
                                   push_mirror_repo_path=push_mirror_repo_path,
                                   infrastructure_commit_author=infrastructure_commit_author,
                                   path_mapper=amendment_path_mapper)
        self._next_ott_id = None
        self._id_minting_file = os.path.join(path, 'next_ott_id.json')

    # rename some generic members in the base class, for clarity and backward compatibility
    @property
    def next_ott_id(self):
        return self._next_ott_id

    def _determine_next_ott_id(self):
        """Read an initial value (int) from our stored counter (file)

        Checks out master branch as a side effect!
        """
        with self._doc_counter_lock:
            _LOG.debug('Reading "{}"'.format(self._id_minting_file))
            noi_contents = self._read_master_branch_resource(self._id_minting_file, is_json=True)
            if noi_contents:
                self._next_ott_id = noi_contents['next_ott_id']
            else:
                raise RuntimeError('Stored ottid minting file not found (or invalid)!')

    def _diagnose_prefixes(self):
        """Returns a set of all of the prefixes seen in the main document dir
           (This is currently always empty, since we don't use a prefix for
           naming taxonomic amendments.)
        """
        return set()

    def _mint_new_ott_ids(self, how_many=1):
        """ ASSUMES the caller holds the _doc_counter_lock !
        Checks the current int value of the next ottid, reserves a block of
        {how_many} ids, advances the counter to the next available value,
        stores the counter in a file in case the server is restarted.
        Checks out master branch as a side effect."""
        first_minted_id = self._next_ott_id
        self._next_ott_id = first_minted_id + how_many
        content = u'{"next_ott_id": %d}\n' % self._next_ott_id
        # The content is JSON, but we hand-rolled the string above
        #       so that we can use it as a commit_msg
        self._write_master_branch_resource(content,
                                           self._id_minting_file,
                                           commit_msg=content,
                                           is_json=False)
        last_minted_id = self._next_ott_id - 1
        return first_minted_id, last_minted_id


class TaxonomicAmendmentStoreProxy(ShardedDocStoreProxy):
    """Proxy for interacting with external resources if given the configuration of a remote TaxonomicAmendmentStore
    """

    def __init__(self, config):
        ShardedDocStoreProxy.__init__(self, config, 'amendments',
                                      path_mapper=amendment_path_mapper,
                                      document_schema=TaxonomicAmendmentDocSchema)


# noinspection PyProtectedMember
class _TaxonomicAmendmentStore(TypeAwareDocStore):
    """Wrapper around a set of sharded git repos.
    """
    id_regex = AmendmentFilepathMapper.id_pattern

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
                                   path_mapper=amendment_path_mapper,
                                   repos_dict=repos_dict,
                                   repos_par=repos_par,
                                   git_shard_class=TaxonomicAmendmentsShard,
                                   mirror_info=mirror_info,
                                   infrastructure_commit_author=infrastructure_commit_author,
                                   **kwargs)
        self._growing_shard._determine_next_ott_id()

    def add_new_doc(self, json_repr, auth_info, commit_msg='', doc_id=None):
        """Validate and save this JSON. Ensure (and return) a unique amendment id"""
        if doc_id is not None:
            raise NotImplementedError("Creating new amendments with pre-assigned IDs is not supported.")
        amendment = self._coerce_json_to_document(json_repr)
        if amendment is None:
            msg = "File failed to parse as JSON:\n{j}".format(j=json_repr)
            raise ValueError(msg)
        if not self._is_valid_document_json(amendment):
            msg = "JSON is not a valid amendment:\n{j}".format(j=json_repr)
            raise ValueError(msg)

        # Mint any needed ottids, update the document accordingly, and
        # prepare a response with
        #  - per-taxon mapping of tag to ottid
        #  - resulting id (or URL) to the stored amendment
        # To ensure synchronization of ottids and amendments, this should be an
        # atomic operation!

        # check for tags and confirm count of new ottids required (if provided)
        num_taxa_eligible_for_ids = 0
        for taxon in amendment.get("taxa"):
            # N.B. We don't require 'tag' in amendment validation; check for it now!
            if "tag" not in taxon:
                raise KeyError('Requested Taxon is missing "tag" property!')
            # allow for taxa that have already been assigned (use cases?)
            if "ott_id" not in taxon:
                num_taxa_eligible_for_ids += 1
        if 'new_ottids_required' in amendment:
            requested_ids = amendment['new_ottids_required']
            try:
                assert (requested_ids == num_taxa_eligible_for_ids)
            except:
                m = 'Number of OTT ids requested ({r}) does not match eligible taxa ({t})'
                m = m.format(r=requested_ids, t=num_taxa_eligible_for_ids)
                raise ValueError(m)

        # mint new ids and assign each to an eligible taxon
        with self._growing_shard._doc_counter_lock:
            # build a map of tags to ottids, to return to the caller
            tag_to_id = {}
            first_new_id = self._growing_shard.next_ott_id
            last_new_id = first_new_id + num_taxa_eligible_for_ids - 1
            if last_new_id < first_new_id:
                # This can happen if ther are no eligible taxa! In this case,
                # repeat and "burn" the next ottid (ie, it will be used to
                # identify this amendment, but it won't be assigned)
                last_new_id = first_new_id
            new_id = first_new_id
            for taxon in amendment.get("taxa"):
                if "ott_id" not in taxon:
                    taxon["ott_id"] = new_id
                    ttag = taxon["tag"]
                    tag_to_id[ttag] = new_id
                    new_id += 1
                    ptag = taxon.get("parent_tag")
                    if ptag is not None:
                        taxon["parent"] = tag_to_id[ptag]
            if num_taxa_eligible_for_ids > 0:
                try:
                    assert (new_id == (last_new_id + 1))
                except:
                    applied = last_new_id - first_new_id + 1
                    m = 'Number of OTT ids requested ({r}) does not match ids actually applied ({a})'
                    m = m.format(r=requested_ids, a=applied)
                    raise ValueError(m)

            # Build a proper amendment id, in the format '{subtype}-{first ottid}-{last-ottid}'
            amendment_subtype = 'additions'
            # TODO: Handle other subtypes (beyond additions) by examining JSON?
            amendment_id = "{s}-{f}-{l}".format(s=amendment_subtype, f=first_new_id, l=last_new_id)

            # Check the proposed id for uniqueness (just to be safe), then
            # "reserve" it using a placeholder value.
            with self._index_lock:
                if amendment_id in self._doc2shard_map:
                    # this should never happen!
                    raise KeyError('Amendment "{i}" already exists!'.format(i=amendment_id))
                self._doc2shard_map[amendment_id] = None

            # Set the amendment's top-level "id" property to match
            amendment["id"] = amendment_id

            # pass the id and amendment JSON to a proper git action
            new_amendment_id = None
            r = None
            try:
                # assign the new id to a shard (important prep for commit_and_try_merge2master)
                gd_id_pair = self.create_git_action_for_new_document(new_doc_id=amendment_id)
                new_amendment_id = gd_id_pair[1]
                # For amendments, the id should not have changed!
                try:
                    assert new_amendment_id == amendment_id
                except:
                    raise KeyError('Amendment id unexpectedly changed from "{o}" to "{n}"!'.format(
                        o=amendment_id, n=new_amendment_id))
                try:
                    # it's already been validated, so keep it simple
                    r = self.commit_and_try_merge2master(file_content=amendment,
                                                         doc_id=new_amendment_id,
                                                         auth_info=auth_info,
                                                         parent_sha=None,
                                                         commit_msg=commit_msg,
                                                         merged_sha=None)
                except:
                    self._growing_shard.delete_doc_from_index(new_amendment_id)
                    raise

                # amendment is now in the repo, so we can safely reserve the ottids
                first_minted_id, last_minted_id = self._growing_shard._mint_new_ott_ids(
                    how_many=max(num_taxa_eligible_for_ids, 1))
                # do a final check for errors!
                try:
                    assert first_minted_id == first_new_id
                except:
                    raise ValueError('First minted ottid is "{m}", expected "{e}"!'.format(
                        m=first_minted_id, e=first_new_id))
                try:
                    assert last_minted_id == last_new_id
                except:
                    raise ValueError('Last minted ottid is "{m}", expected "{e}"!'.format(
                        m=last_minted_id, e=last_new_id))
                # Add the tag-to-ottid mapping to the response, so a caller
                # (e.g. the curation webapp) can provisionally assign them
                r['tag_to_ottid'] = tag_to_id
            except:
                with self._index_lock:
                    if new_amendment_id in self._doc2shard_map:
                        del self._doc2shard_map[new_amendment_id]
                raise

        with self._index_lock:
            self._doc2shard_map[new_amendment_id] = self._growing_shard
        return new_amendment_id, r

    add_new_amendment = add_new_doc

    def _build_amendment_id(self, json_repr):
        """Parse the JSON, return a slug in the form '{subtype}-{first ottid}-{last-ottid}'."""
        amendment = self._coerce_json_to_document(json_repr)
        if amendment is None:
            return None
        amendment_subtype = 'additions'
        # TODO: Look more deeply once we have other subtypes!
        first_ottid = amendment['TODO']
        last_ottid = amendment['TODO']
        return slugify('{s}-{f}-{l}'.format(s=amendment_subtype, f=first_ottid, l=last_ottid))


_THE_TAXONOMIC_AMENDMENT_STORE = None


# noinspection PyPep8Naming
def TaxonomicAmendmentStore(repos_dict=None,
                            repos_par=None,
                            mirror_info=None,
                            infrastructure_commit_author='OpenTree API <api@opentreeoflife.org>'):
    """Factory function for a _TaxonomicAmendmentStore object.

    A wrapper around the _TaxonomicAmendmentStore class instantiation for
    the most common use case: a singleton _TaxonomicAmendmentStore.
    If you need distinct _TaxonomicAmendmentStore objects, you'll need to
    call that class directly.
    """
    global _THE_TAXONOMIC_AMENDMENT_STORE
    if _THE_TAXONOMIC_AMENDMENT_STORE is None:
        r = _TaxonomicAmendmentStore(repos_dict=repos_dict,
                                     repos_par=repos_par,
                                     mirror_info=mirror_info,
                                     infrastructure_commit_author=infrastructure_commit_author)
        _THE_TAXONOMIC_AMENDMENT_STORE = r
    return _THE_TAXONOMIC_AMENDMENT_STORE


def create_taxonomic_amendments_umbrella(shard_mirror_pair_list):
    return _TaxonomicAmendmentStore(shard_mirror_pair_list=shard_mirror_pair_list)
'''
