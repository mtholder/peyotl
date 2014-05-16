#!/usr/bin/env python
from cStringIO import StringIO
from peyotl.utility import get_config
import datetime
import requests
import json
import gzip
from peyotl import get_logger
_LOG = get_logger(__name__)

_GZIP_REQUEST_HEADERS = {
    'accept-encoding' : 'gzip',
    'content-type' : 'application/json',
    'accept' : 'application/json',
}

_JSON_HEADERS = {'content-type': 'application/json'}

class APIDomains(object):
    def __init__(self):
        self._phylografter = 'http://www.reelab.net/phylografter'
        self._doc_store = None
    def get_phylografter(self):
        return self._phylografter
    phylografter = property(get_phylografter)
    def get_doc_store(self):
        if self._doc_store is None:
            self._doc_store = get_config('apis', 'doc_store')
            if self._doc_store is None:
                raise RuntimeError('[apis] / doc_store config setting required')
        return self._doc_store
    doc_store = property(get_doc_store)

def get_domains_obj():
    # hook for config/env-sensitive setting of domains
    api_domains = APIDomains()
    return api_domains

class APIWrapper(object):
    def __init__(self, domains=None):
        if domains is None:
            domains = get_domains_obj()
        self.domains = domains
        self._phylografter = None
        self._doc_store = None
    def get_phylografter(self):
        if self._phylografter is None:
            self._phylografter = _PhylografterWrapper(self.domains.phylografter)
        return self._phylografter
    phylografter = property(get_phylografter)
    def get_doc_store(self):
        if self._doc_store is None:
            self._doc_store = _DocStoreAPIWrapper(self.domains.doc_store)
        return self._doc_store
    doc_store = property(get_doc_store)

class _WSWrapper(object):
    def __init__(self, domain):
        self.domain = domain
    def _get(self, url, headers=_JSON_HEADERS, params=None):
        resp = requests.get(url, params=params, headers=headers)
        resp.raise_for_status()
        try:
            return resp.json()
        except:
            return resp.json

class _DocStoreAPIWrapper(_WSWrapper):
    def __init__(self, domain):
        _WSWrapper.__init__(self, domain)
    def study_list(self):
        SUBMIT_URI = '{}/study_list'.format(self.domain)
        return self._get(SUBMIT_URI)
    def unmerged_branches(self):
        SUBMIT_URI = '{}/unmerged_branches'.format(self.domain)
        return self._get(SUBMIT_URI)

class _PhylografterWrapper(_WSWrapper):
    def __init__(self, domain):
        _WSWrapper.__init__(self, domain)
    def get_modified_list(self, since_date="2010-01-01T00:00:00"):
        '''Calls phylografter's modified_list.json to fetch
        a list of all studies that have changed since `since_date`
        `since_date` can be a datetime.datetime object or a isoformat
        string representation of the time.
        '''
        if isinstance(since_date, datetime.datetime):
            since_date = datetime.isoformat(since_date)
        SUBMIT_URI = self.domain + '/study/modified_list.json/url'
        args = {'from': since_date}
        return self._get(SUBMIT_URI, params=args)

    def get_nexson(self, study_id):
        '''Calls export_gzipNexSON URL and unzips response.
        Raises HTTP error, gzip module error, or RuntimeError
        '''
        if study_id.startswith('pg_'):
            study_id = study_id[3:] #strip pg_ prefix
        SUBMIT_URI = self.domain + '/study/export_gzipNexSON.json/' + study_id
        _LOG.debug('Downloading %s using "%s"\n' % (study_id, SUBMIT_URI))
        resp = requests.get(SUBMIT_URI,
                            headers=_GZIP_REQUEST_HEADERS,
                            allow_redirects=True)
        resp.raise_for_status()
        try:
            uncompressed = gzip.GzipFile(mode='rb',
                                         fileobj=StringIO(resp.content)).read()
            results = uncompressed
        except:
            raise 
        if isinstance(results, unicode) or isinstance(results, str):
            return json.loads(results)
        raise RuntimeError('gzipped response from phylografter export_gzipNexSON.json, but not a string is:', results)


def NexsonStore(domains=None):
    return APIWrapper(domains=domains).doc_store

def Phylografter(domains=None):
    return APIWrapper(domains=domains).phylografter
