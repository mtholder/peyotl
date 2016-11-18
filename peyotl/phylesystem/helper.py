from peyotl.utility import get_logger
from peyotl.phylesystem.git_actions import get_filepath_for_namespaced_id, get_filepath_for_simple_id
import os
import re
from threading import Lock

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


def _initialize_study_index(repos_par=None, **kwargs):
    d = {}  # Key is study id, value is repo,dir tuple
    repos = get_repos(repos_par)
    for repo in repos:
        p = os.path.join(repos[repo], 'study')
        dr = create_id2study_info(p, repo)
        d.update(dr)
    return d


DIGIT_PATTERN = re.compile(r'^\d')

_CACHE_REGION_CONFIGURED = False
_REGION = None


def _make_phylesystem_cache_region(**kwargs):
    """Only intended to be called by the Phylesystem singleton.
    """
    global _CACHE_REGION_CONFIGURED, _REGION
    if _CACHE_REGION_CONFIGURED:
        return _REGION
    _CACHE_REGION_CONFIGURED = True
    try:
        # noinspection PyPackageRequirements
        from dogpile.cache import make_region
    except:
        _LOG.debug('dogpile.cache not available')
        return
    region = None
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
            region = None
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
