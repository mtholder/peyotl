#!/usr/bin/env python
# Some imports to help our py2 code behave like py3
from __future__ import absolute_import, print_function, division

import re

from .utility import (UNICODE)

_EMPTY_TUPLE = tuple
NEWICK_NEEDING_QUOTING = re.compile(r'(\s|[\[\]():,;])')


def quote_newick_name(s, needs_quotes_pattern=NEWICK_NEEDING_QUOTING):
    s = UNICODE(s)
    if "'" in s:
        return u"'{}'".format("''".join(s.split("'")))
    if needs_quotes_pattern.search(s):
        return u"'{}'".format(s)
    return s
