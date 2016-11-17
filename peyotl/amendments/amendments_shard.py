import os
import codecs
from threading import Lock
from peyotl.utility import get_logger
from peyotl.git_storage.git_shard import (GitShard,
                                          TypeAwareGitShard)
from peyotl.git_storage.type_aware_doc_store import SimpleJSONDocSchema
from peyotl.amendments.validation import validate_amendment

_LOG = get_logger(__name__)

doc_holder_subpath = 'amendments'


def filepath_for_amendment_id(repo_dir, amendment_id):
    # in this case, simply expand the id to a full path
    amendment_filename = '{i}.json'.format(i=amendment_id)
    full_path_to_file = os.path.join(repo_dir, doc_holder_subpath, amendment_filename)
    _LOG.warn(">>>> filepath_for_amendment_id: full path is {}".format(full_path_to_file))
    return full_path_to_file


class TaxonomicAmendmentDocSchema(SimpleJSONDocSchema):
    def __init__(self):
        SimpleJSONDocSchema.__init__(self, document_type='taxon amendment JSON')

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

    def validate_annotate_convert_doc(self, document, **kwargs):
        """No conversion between different schema is supported for amendments"""
        errors, adaptor = validate_amendment(document)
        return document, errors, None, adaptor


class TaxonomicAmendmentsShardProxy(GitShard):
    """Proxy for shard when interacting with external resources if given the configuration of a remote Phylesystem
    """

    def __init__(self, config):
        GitShard.__init__(self, config['name'], doc_schema=TaxonomicAmendmentDocSchema())
        d = {}
        for amendment in config['amendments']:
            kl = amendment['keys']
            if len(kl) > 1:
                _LOG.warn("aliases not supported in shards")
            for k in amendment['keys']:
                complete_path = '{p}/{s}/{r}'.format(p=self.path, s=doc_holder_subpath, r=amendment['relpath'])
                d[k] = (self.name, self.path, complete_path)
        self.doc_index = d


def create_id2amendment_info(path, tag):
    """Searches for JSON files in this repo and returns
    a map of amendment id ==> (`tag`, dir, amendment filepath)
    where `tag` is typically the shard name
    """
    d = {}
    for triple in os.walk(path):
        root, files = triple[0], triple[2]
        for filename in files:
            if filename.endswith('.json'):
                # trim its file extension 
                amendment_id = n = filename[:-5]
                d[amendment_id] = (tag, root, os.path.join(root, filename))
    return d


def refresh_amendment_index(shard, initializing=False):
    d = create_id2amendment_info(shard.doc_dir, shard.name)
    shard._doc_index = d


class TaxonomicAmendmentsShard(TypeAwareGitShard):
    """Wrapper around a git repo holding JSON taxonomic amendments
    Raises a ValueError if the directory does not appear to be a TaxonomicAmendmentsShard.
    Raises a RuntimeError for errors associated with misconfiguration."""
    from peyotl.phylesystem.git_actions import PhylesystemGitAction
    document_type = 'taxon_amendment'

    def __init__(self,
                 name,
                 path,
                 git_ssh=None,
                 pkey=None,
                 git_action_class=PhylesystemGitAction,
                 push_mirror_repo_path=None,
                 infrastructure_commit_author='OpenTree API <api@opentreeoflife.org>'):
        TypeAwareGitShard.__init__(self,
                                   name=name,
                                   path=path,
                                   doc_holder_subpath=doc_holder_subpath,
                                   doc_schema=TaxonomicAmendmentDocSchema(),
                                   refresh_doc_index_fn=refresh_amendment_index,  # populates _doc_index
                                   git_ssh=git_ssh,
                                   pkey=pkey,
                                   git_action_class=git_action_class,
                                   push_mirror_repo_path=push_mirror_repo_path,
                                   infrastructure_commit_author=infrastructure_commit_author)
        self.filepath_for_doc_id_fn = filepath_for_amendment_id
        self._doc_counter_lock = Lock()
        self._next_ott_id = None
        self._id_minting_file = os.path.join(path, 'next_ott_id.json')
        # N.B. This is for minting invididual taxon (OTT) ids, not amendment ids!
        # We construct each amendment from its unique series of ottids.
        self.filepath_for_global_resource_fn = lambda frag: os.path.join(path, frag)

    # rename some generic members in the base class, for clarity and backward compatibility
    @property
    def next_ott_id(self):
        return self._next_ott_id

    @property
    def known_prefixes(self):
        # this is required by TypeAwareDocStore
        if self._known_prefixes is None:
            self._known_prefixes = self._diagnose_prefixes()
        return self._known_prefixes

    def _determine_next_ott_id(self):
        """Read an initial value (int) from our stored counter (file)

        Checks out master branch as a side effect!
        """
        if self._doc_counter_lock is None:
            self._doc_counter_lock = Lock()
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

    def create_git_action_for_new_amendment(self, new_amendment_id=None):
        """Checks out master branch as a side effect"""
        ga = self.create_git_action()
        assert new_amendment_id is not None
        # id should have been sorted out by the caller
        self.register_doc_id(ga, new_amendment_id)
        return ga, new_amendment_id

    def _create_git_action_for_global_resource(self):
        return self._ga_class(repo=self.path,
                              git_ssh=self.git_ssh,
                              pkey=self.pkey,
                              path_for_doc_fn=self.filepath_for_global_resource_fn,
                              max_file_size=self.max_file_size)
