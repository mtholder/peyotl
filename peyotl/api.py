#!/usr/bin/env python
# Some imports to help our py2 code behave like py3
from __future__ import absolute_import, print_function, division

from .jobs import OTC_TOL_WS
from .tree_services import OTCWrapper

SERVICE_NAME_TO_WRAPPER = {OTC_TOL_WS: OTCWrapper}
