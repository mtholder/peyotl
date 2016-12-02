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
