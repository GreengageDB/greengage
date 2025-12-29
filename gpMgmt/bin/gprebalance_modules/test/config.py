import os

from gppylib.gparray import Segment, GpArray
from gprebalance_modules.planner import ConfigurationEncoder, Planner, HostResolver


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
            target_hostnames = None
            add_hostnames = None
            remove_hostnames = None
            if target_hosts:
                target_hostnames = [h.strip() for h in target_hosts.split(',')]
            if add_hosts:
                add_hostnames = [h.strip() for h in add_hosts.split(',')]
            if remove_hosts:
                remove_hostnames = [h.strip() for h in remove_hosts.split(',')]
            self.encoding = ConfigurationEncoder.encode_configuration(gparray,
                                                                     Planner.get_target_hosts(
                                                                            array=gparray,
                                                                            target_hostname_list=target_hostnames,
                                                                            add_hostname_list=add_hostnames,
                                                                            remove_hostname_list=remove_hostnames)[0],
                                                                     strat)
            res = func(self, *args, **kwargs)
            self.encoding = None
            return res
        return wrapper
    return inner
