#!/usr/bin/env python
# Some imports to help our py2 code behave like py3
from __future__ import absolute_import, print_function, division
import os
from .utility import CfgSettingType, read_as_json
from .api_wrappers import APIWrapper, WrapperMode
from .jobs import OTC_TOL_WS
from .utility import logger

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
        import json
        for test_name, test_descrip in test_dict.items():
            func = test_descrip['test_function']
            outcome = test_descrip['tests']
            input = test_descrip['test_input']
            print('{} says that {}({}) ==> {}'.format(test_name, func, input, outcome))

        return all_run, all_passed