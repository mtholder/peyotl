#!/usr/bin/env python
# Some imports to help our py2 code behave like py3
from __future__ import absolute_import, print_function, division
import os
from .utility import CfgSettingType, read_as_json
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

    def mrca(self, node_ids=None, ott_ids=None):
        if not node_ids:
            if not ott_ids:
                raise ValueError("node_ids or ott_ids must be supplied to mrca")
            d = {'ott_ids': ott_ids}
        elif bool(ott_ids):
            raise ValueError("Only one of node_ids or ott_ids may be supplied to mrca")
        else:
            d = {'node_ids': node_ids}
        return json_http_post_raise(url=self.url_for('tree_of_life/mrca'), data=d)

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


    def _run_shared_api_tests(self, repo_dir, out):
        all_run = True
        all_passed = True
        fp = os.path.join(repo_dir, 'tree_of_life.json')
        try:
            test_dict = read_as_json(fp)
        except:
            logger(__name__).warn('tests skipped due to missing {}'.format(fp))
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
