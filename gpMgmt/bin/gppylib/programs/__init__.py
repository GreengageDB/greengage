# Make sure Python loads the modules of this package via absolute paths.
from __future__ import absolute_import
from os.path import abspath as _abspath

__path__[0] = _abspath(__path__[0])
