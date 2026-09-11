# pygresql/__init__.py
from __future__ import absolute_import
import sys
import pg, pgdb
sys.modules.setdefault('pygresql.pg', pg)
sys.modules.setdefault('pygresql.pgdb', pgdb)

