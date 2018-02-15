#!/usr/bin/env python
# Some imports to help our py2 code behave like py3
from __future__ import absolute_import, print_function, division
import os
from .utility import (CfgSettingType, is_str_type, read_as_json)
from .api_wrappers import APIWrapper, WrapperMode
from .jobs import OTC_TOL_WS
from .utility import logger
from .http_helper import json_http_post_raise


class OTCWrapper(APIWrapper):
    def __init__(self):
        APIWrapper.__init__(self, service=OTC_TOL_WS)
        self._settings = self._cfg.get_setting([OTC_TOL_WS])
        self.base_url = self._settings.get('base_url')
        if self.base_url is None:
            self._mode = WrapperMode.LOCAL_WS
            p = self._settings.get('port', '1985')
            prefix = self._settings.get('prefix', '')
            self.base_url = 'http://127.0.0.1:{}/{}'.format(p, prefix)
        else:
            self._mode = WrapperMode.REMOTE_WS
        logger(__name__).debug('OTCWrapper configured to have base_url={}'.format(self.base_url))

    def about(self, include_source_list=False):
        d = {'include_source_list': include_source_list}
        return json_http_post_raise(url=self.url_for('tree_of_life/about'), data=d)

    @staticmethod
    def _validate_node_ids_or_ott_ids(node_ids, ott_ids, method):
        if not node_ids:
            if not ott_ids:
                raise ValueError("node_ids or ott_ids must be supplied to {}".format(method))
            return {'node_ids': ['ott{}'.format(i) for i in ott_ids]}
        if not isinstance(node_ids, list):
            if is_str_type(node_ids):
                raise ValueError("Expecting node_ids to be an iterable collection of strings")
            node_ids = list(node_ids)
        if ott_ids:
            node_ids.extend(['ott{}'.format(i) for i in ott_ids])
        return {'node_ids': node_ids}

    @staticmethod
    def _validate_node_id_or_ott_id(node_id, ott_id, method):
        if node_id is None:
            if ott_id is None:
                raise ValueError('node_id or ott_id required by {} method'.format(method))
            return {'node_id': 'ott{}'.format(ott_id)}
        if ott_id is not None:
            raise ValueError(
                'only 1 of node_id and ott_id can be given to the {} method'.format(method))
        return {'node_id': node_id}

    @staticmethod
    def _add_label_format_if_valid(d, label_format, method):
        if label_format not in {'name_and_id', 'name', 'id'}:
            raise ValueError('Unkown label_format "{}" in {} method'.format(label_format, method))
        d['label_format'] = label_format

    def mrca(self, node_ids=None, ott_ids=None):
        d = self._validate_node_ids_or_ott_ids(node_ids, ott_ids, 'mrca')
        return json_http_post_raise(url=self.url_for('tree_of_life/mrca'), data=d)

    def induced_subtree(self, node_ids=None, ott_ids=None, label_format="name_and_id"):
        d = self._validate_node_ids_or_ott_ids(node_ids, ott_ids, 'induced_subtree')
        self._add_label_format_if_valid(d, label_format, 'induced_subtree')
        return json_http_post_raise(url=self.url_for('tree_of_life/induced_subtree'), data=d)

    # noinspection PyShadowingBuiltins
    def subtree(self,
                node_id=None,
                ott_id=None,
                format='newick',
                label_format="name_and_id",
                height_limit=None):
        d = self._validate_node_id_or_ott_id(node_id, ott_id, 'subtree')
        if format == 'newick':
            if height_limit is None:
                height_limit = -1
            self._add_label_format_if_valid(d, label_format, 'subtree')
        elif format == 'arguson':
            if height_limit is None:
                height_limit = 3
        else:
            raise ValueError('unknown format "{}" in subtree'.format(format))
        d['height_limit'] = height_limit
        d['format'] = format
        return json_http_post_raise(url=self.url_for('tree_of_life/subtree'), data=d)

    def _get_local_invocation(self):
        cfg = self._cfg
        ott_dir = cfg.get_setting(['ott', 'directory'],
                                  raise_on_none=True,
                                  type_check=CfgSettingType.EXISTING_DIR)
        synth_par = cfg.get_setting(['synthpar', 'directory'],
                                    raise_on_none=True,
                                    type_check=CfgSettingType.EXISTING_DIR)
        otc_settings = cfg.get_setting(['otcws'], raise_on_none=True)
        port_num = otc_settings.get('port', 1984)
        num_threads = otc_settings.get('num_threads', 1)
        prefix = otc_settings.get('prefix', '')
        invoc = ['otc-tol-ws',
                 os.path.abspath(ott_dir),
                 '--tree-dir={}'.format(os.path.abspath(synth_par)),
                 '--port={}'.format(port_num),
                 '--num-thread={}'.format(num_threads)
                 ]
        if prefix:
            invoc.append('--prefix={}'.format(prefix))
        return invoc

    # noinspection PyShadowingBuiltins
    def _run_shared_api_tests(self, repo_dir, out):
        all_run = True
        all_passed = True
        fp = os.path.join(repo_dir, 'tree_of_life.json')
        try:
            test_dict = read_as_json(fp)
        except:
            logger(__name__).warn('tests skipped due to missing {}'.format(fp))
            return False, True
        for test_name, test_descrip in test_dict.items():
            mapped = self._map_shared_api_test_name_to_method(test_descrip)
            if mapped is None:
                all_run = False
                logger(__name__).info('Skipping {}'.format(test_name))
            else:
                outcome = test_descrip['tests']
                input = test_descrip['test_input']
                if not self._call_method_from_shared_test(out,
                                                          test_name,
                                                          mapped,
                                                          input,
                                                          outcome):
                    all_passed = False
        return all_run, all_passed

    def _map_shared_api_test_name_to_method(self, test_descrip_dict):
        decorated_method_name = test_descrip_dict['test_function']
        shared_test_pref = 'tol_'
        if decorated_method_name.startswith(shared_test_pref):
            method_name = decorated_method_name[len(shared_test_pref):]
            try:
                return getattr(self, method_name)
            except AttributeError:
                m = 'method {} missing from {}'.format(method_name, type(self))
                logger(__name__).exception(m)
        return None
