#!/usr/bin/env python3

from dataclasses import dataclass
from typing import Set, Optional
from enum import IntEnum
from gppylib.gparray import Segment 

class HostStatus(IntEnum):
    ACTIVE = 1
    NEW = 2
    DECOMMISSIONED = 3


@dataclass
class Host:
    """
    Segment host representaion
    
    Attributes:
        hostname: hostname from gp_segment_configuration
        address: address from gp_segment_configuration
        primary_datadirs: set of datadirs which primary catalogs belong to
        mirror_datadirs: set of datadirs which mirror catalogs belong to
        status: intendend host usage
    """
    hostname: str
    address: str
    primary_datadirs: Set[str] = None
    mirror_datadirs: Set[str] = None
    status: HostStatus = None

    def __hash__(self):
        return hash((self.hostname, self.address))
    
    def __eq__(self, other):
        if not isinstance(other, Host):
            return NotImplemented
        return self.hostname == other.hostname and self.address == other.address

    def __str__(self):
        pass

class SegmentSize:
    """
    TODO. Should contain storage size of
    segment instance with all tablespace info.
    """
    pass

@dataclass
class CandidateSegment:
    """
    Accumulates the info of rebalance segments
    
    Attributes:
        info: segment info from gparray
        size: size estimation before execution
        host: placement host
    """
    info: Segment
    host: Host
    size: Optional[SegmentSize]