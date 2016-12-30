"""Base class for sharded document storage (for phylesystem, tree collections, etc.)
N.B. that this class has no knowledge of different document types. It provides
basic functionality common to minimal *Proxy classes with remote shards, and
more full-featured subclasses based on TypeAwareDocStore.
"""
from threading import Lock
from peyotl.git_storage.git_shard import GitShardProxy
from peyotl.utility import get_logger

_LOG = get_logger(__name__)


class ShardedDocStore(object):
    """Shared functionality for PhylesystemBase, TreeCollectionStoreBase, etc.
    We'll use 'doc' here to refer to a single object of interest (eg, a study or
    tree collection) in the collection.

    N.B. In current subclasses, each docstore has one main document type, and
    each document is stored as a single file in git. Watch for complications if
    either of these assumptions is challenged for a new type!
    """

    def __init__(self, path_mapper=None):
        self._index_lock = Lock()
        self._shards = []
        self._doc2shard_map = {}
        self._prefix2shard = {}
        self.path_mapper = path_mapper
        self._growing_shard = None

    def get_repo_and_path_fragment(self, doc_id):
        """For `doc_id` returns a list of:
            [0] the repo name and,
            [1] the path from the repo to the doc file.
        This is useful because
        (if you know the remote), it lets you construct the full path.
        """
        shard = self.get_shard(doc_id)
        return shard.name, shard.get_rel_path_fragment(doc_id)

    def get_public_url(self, doc_id, branch='master'):
        """Returns a GitHub URL for the doc in question (study, collection, ...)
        """
        name, path_frag = self.get_repo_and_path_fragment(doc_id)
        return 'https://raw.githubusercontent.com/OpenTreeOfLife/' + name + '/' + branch + '/' + path_frag

    get_external_url = get_public_url

    def _doc_merged_hook(self, ga, doc_id):
        with self._index_lock:
            if doc_id in self._doc2shard_map:
                return
        # this lookup has to be outside of the lock-holding part to avoid deadlock
        shard = self.get_shard(doc_id)
        with self._index_lock:
            self._doc2shard_map[doc_id] = shard
        try:
            shard.register_doc_id(ga, doc_id)
        except AttributeError:
            pass

    def get_shard(self, doc_id):
        try:
            with self._index_lock:
                return self._doc2shard_map[doc_id]
        except KeyError:
            for s in self._shards:
                if s.had_doc_id(doc_id):
                    _LOG.debug('Shard at "{}" had "{}"'.format(s.path, doc_id))
                    return s
            _LOG.debug('No shard at "{}" had "{}"'.format(s.path, doc_id))
            raise

    def get_doc_ids(self):
        k = []
        for shard in self._shards:
            k.extend(shard.get_doc_ids())
        return k

    def get_doc_filepath(self, doc_id):
        shard = self.get_shard(doc_id)
        return shard.get_doc_filepath(doc_id)


class ShardedDocStoreProxy(ShardedDocStore):
    def __init__(self, config, config_key, path_mapper, document_schema):
        ShardedDocStore.__init__(self, path_mapper=path_mapper)
        for s in config.get('shards', []):
            sp = GitShardProxy(s, config_key, path_mapper=path_mapper, document_schema=document_schema)
            self._shards.append(sp)
        self._doc2shard_map = None
        self.create_doc_index(config_key)

    def create_doc_index(self, config_key):
        d = {}
        for s in self._shards:
            for k in s.doc_index.keys():
                if k in d:
                    msg = '{c} element "{i}" found in multiple repos'.format(c=config_key, i=k)
                    raise KeyError(msg)
                d[k] = s
        self._doc2shard_map = d
