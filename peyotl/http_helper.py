#!/usr/bin/env python
# Some imports to help our py2 code behave like py3
from __future__ import absolute_import, print_function, division

import json
import os
from codecs import open

import requests

from .utility import logger, is_str_type, UNICODE

CURL_LOGGER = os.environ.get('PEYOTL_CURL_LOG_FILE')


def escape_dq(s):
    if not is_str_type(s):
        if isinstance(s, bool):
            if s:
                return 'true'
            return 'false'

        return s
    if '"' in s:
        ss = s.split('"')
        return '"{}"'.format('\\"'.join(ss))
    return '"{}"'.format(s)


def log_request_as_curl(curl_log, url, verb, headers, params, data):
    if not curl_log:
        return
    with open(curl_log, 'a', encoding='utf-8') as curl_fo:
        if headers:
            curl_h = ['-H {}:{}'.format(escape_dq(k), escape_dq(v)) for k, v in headers.items()]
            hargs = ' '.join(curl_h)
        else:
            hargs = ''
        if params and not data:
            import urllib
            url = url + '?' + urllib.urlencode(params)
        if data:
            if is_str_type(data):
                data = json.loads(data)
            dargs = "'" + json.dumps(data) + "'"
        else:
            dargs = ''
        data_arg = ''
        if dargs:
            data_arg = ' --data {d}'.format(d=dargs)
        curl_fo.write('curl -X {v} {h} {u}{d}\n'.format(v=verb,
                                                        u=url,
                                                        h=hargs,
                                                        d=data_arg))


_CUTOFF_LEN_DETAILED_VIEW = 500


def _dict_summary(d, name):
    dk = list(d.keys())
    dk.sort()
    sd = UNICODE(d)
    if len(sd) < _CUTOFF_LEN_DETAILED_VIEW:
        a = []
        for k in dk:
            a.extend([repr(k), ': ', repr(d[k]), ', '])
        return u'%s={%s}' % (name, ''.join(a))
    return u'%s-keys=%s' % (name, repr(dk))


def _http_method_summary_str(url, verb, headers, params, data=None):
    if params is None:
        ps = 'None'
    else:
        ps = _dict_summary(params, 'params')
    hs = _dict_summary(headers, 'headers')
    if data is None:
        ds = 'None'
    elif is_str_type(data):
        ds = _dict_summary(json.loads(data), 'data')
    else:
        ds = _dict_summary(data, 'data')
    fmt = 'error in HTTP {v} verb call to {u} with {p}, {d} and {h}'
    return fmt.format(v=verb, u=url, p=ps, h=hs, d=ds)


_VERB_TO_METHOD_DICT = {
    'GET': requests.get,
    'POST': requests.post,
    'PUT': requests.put,
    'DELETE': requests.delete
}


def _do_http(url, verb, headers, params, data, text=False):  # pylint: disable=R0201
    if CURL_LOGGER is not None:
        log_request_as_curl(CURL_LOGGER, url, verb, headers, params, data)
    func = _VERB_TO_METHOD_DICT[verb]
    try:
        resp = func(url, params=params, headers=headers, data=data)
    except requests.exceptions.ConnectionError:
        raise RuntimeError('Could not connect in call of {v} to "{u}"'.format(v=verb, u=url))

    try:
        resp.raise_for_status()
    except:
        logger(__name__).exception(_http_method_summary_str(url, verb, headers, params))
        if resp.text:
            logger(__name__).debug('HTTPResponse.text = ' + resp.text)
        raise
    if text:
        return resp.text
    return resp.json()
