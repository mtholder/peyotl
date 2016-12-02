#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""Some import statements that vary across python versions"""

# This is called from within get_logger/get_config. So, don't use logging here.

try:
    # noinspection PyCompatibility
    from ConfigParser import SafeConfigParser
except ImportError:
    # noinspection PyCompatibility,PyUnresolvedReferences
    from configparser import ConfigParser as SafeConfigParser  # pylint: disable=F0401

try:
    # noinspection PyCompatibility
    from cStringIO import StringIO
except ImportError:
    from io import StringIO  # pylint: disable=E0611,W0403

try:
    # noinspection PyCompatibility
    from HTMLParser import HTMLParser
except ImportError:
    from html.parser import HTMLParser  # pylint: disable=E0611,W0403


# MLStripper and strip_tag function taken from
# http://stackoverflow.com/a/925630
class MLStripper(HTMLParser):
    def __init__(self):
        self.reset()
        self.strict = False
        self.convert_charrefs= True
        self.fed = []
    def handle_data(self, d):
        self.fed.append(d)
    def get_data(self):
        return ''.join(self.fed)

def strip_tags(html):
    s = MLStripper()
    s.feed(html)
    return s.get_data()
