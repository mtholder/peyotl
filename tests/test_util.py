#!/usr/bin/env pytest
# -*- coding: utf-8 -*-
from __future__ import print_function, division
from peyotl import (
    assure_dir_exists,
    configure_logger,
    # download, download_large_file,
    expand_path,
    flush_utf_8_writer,
    get_utf_8_string_io_writer, get_utf_8_value,
    # gunzip, gunzip_and_untar,
    is_str_type, is_int_type, increment_slug,
    open_for_group_write,
    opentree_config_dir,
    pretty_dict_str,
    read_as_json,
    read_filepath,
    reverse_dict,
    shorter_fp_form,
    slugify,
    underscored2camel_case, UNICODE,
    unzip,
    write_as_json,
    write_pretty_dict_str,
    write_to_filepath,
   )
import logging
import os


def test_assure_dir():
    tmp_dir_name = 'bogus'
    if not os.path.exists(tmp_dir_name):
        assure_dir_exists(tmp_dir_name)
        try:
            assert os.path.isdir(tmp_dir_name)
        finally:
            os.rmdir(tmp_dir_name)

def test_pretty_dict():
    assert pretty_dict_str({'a':1, u'b α':2}) == u'{\n  "a": 1,\n  "b α": 2\n}'

def test_expand():
    if 'HOME' in os.environ:
        assert expand_path('${HOME}/bogus') == expand_path('~/bogus')

def test_logger():
    prev = os.environ.get('PEYOTL_LOG_INI_FILEPATH')
    os.environ['PEYOTL_LOG_INI_FILEPATH'] = '/ bogus'
    fp = configure_logger(default_level=logging.DEBUG)
    assert fp is None
    if prev is None:
        del os.environ['PEYOTL_LOG_INI_FILEPATH']
    else:
        os.environ['PEYOTL_LOG_INI_FILEPATH'] = prev

def test_unicode():
    assert type(UNICODE('dga')) is type(u'gag')

def test_is_str_type():
    assert is_str_type('bogus')
    assert is_str_type(u'bogus')
    assert not is_str_type(5)

def test_is_int_type():
    assert not is_int_type('bogus')
    assert not is_int_type('1')
    assert not is_int_type(1.0)
    assert is_int_type(10)
    assert is_int_type(123561235612356123561235612356123561235612356)

def test_utf_writer():
    s, w = get_utf_8_string_io_writer()
    m = u'Greeks start with α'
    w.write(m)
    w.write(m)
    flush_utf_8_writer(w)
    o = get_utf_8_value(s)
    assert o == (m + m)

def test_slugs():
    z = slugify("Trees about bees")
    assert z == 'trees-about-bees'
    n = increment_slug(z)
    assert n == 'trees-about-bees-2'
    nn = increment_slug(n)
    assert nn == 'trees-about-bees-3'

def test_reverse_dict():
    assert reverse_dict({'a': 1, 'b': 2}) == {1:'a', 2: 'b'}

def test_underscored_to_cc():
    assert underscored2camel_case('a_bel_ch') == 'aBelCh'
    assert underscored2camel_case('a_bEl_ch') == 'aBElCh'
    assert underscored2camel_case('a_12_ch') == 'a12Ch'

def test_opentree_config_dir():
    d, qm = opentree_config_dir()
    assert is_str_type(d)
    assert isinstance(qm, list)