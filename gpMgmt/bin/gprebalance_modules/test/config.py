import os
import types

from typing import List, Tuple, Dict
from collections import defaultdict
from gppylib.gparray import Segment, GpArray
from ..planner import ConfigurationEncoder, Planner

def initGparrayFromFile(basename):
    filename = os.path.dirname(__file__) + \
        "/data/" + basename + ".array"
    segdbs = []
    with open(filename, 'r') as fp:
        for line in fp:
            if not line.lstrip().startswith('#'):
                segdbs.append(Segment.initFromString(line))
    return GpArray(segdbs, segdbs)

def getEncoding(file, strat, target_hosts, add_hosts, remove_hosts):
    def inner(func):
        def wrapper(self, *args, **kwargs):
            gparray = initGparrayFromFile(file)
            self.encoding = ConfigurationEncoder.encode_configuration(gparray,
                                                                     Planner.get_target_hosts(
                                                                     gparray,
                                                                     types.SimpleNamespace(target_hosts=target_hosts,
                                                                                           add_hosts=add_hosts,
                                                                                           remove_hosts=remove_hosts) )[0],
                                                                     strat)
            res = func(self, *args, **kwargs)
            self.encoding = None
            return res
        return wrapper
    return inner
