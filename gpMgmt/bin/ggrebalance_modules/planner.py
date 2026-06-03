#!/usr/bin/env python3
#
# Copyright (c) 2025-Present, Greengage Community
#

from collections import defaultdict
import copy
import os
import pickle
from typing import Any, Tuple, List, Dict
from contextlib import closing
from dataclasses import dataclass, field
from copy import deepcopy
import math

from gppylib.db import dbconn
import gppylib.gparray as gparray
from ggrebalance_modules.rebalance_commons import *
from ggrebalance_modules.solver import LNS, HostId, SolverConfig, LNSConfig
from gppylib.commands.unix import PortIsAvailable

class PlanningError(Exception):
    pass

GROUPED = 'grouped'
SPREAD = 'spread'

HostMapping = Dict[Host, HostId]

MAX_SOLVER_ITERS = 1200
SOLVER_TIMEOUT_SEC = 60.0

@dataclass
class LogicalMove:
    """
    Move operation assumed by rebalance
    
    Attributes:
        seg: segment info
        dst: destination host
        target_datadir: target data directory
        target_port: target port
        segment_size: segment volume including tablespaces
        swap_metadata: defines the order of swap moves
    """
    seg: Segment
    srcHost: Host
    dstHost: Host
    target_datadir: str
    target_port: int
    segment_size: Optional[SegmentSize] = None
    swap_metadata: Optional[Dict[str, Any]] = None

    def __str__(self):
        """Pretty print logical move"""
        
        # Source information
        src_host = self.seg.getSegmentHostName()
        src_datadir = self.seg.getSegmentDataDirectory()
        src_port = self.seg.getSegmentPort()
        
        # Destination information
        dst_host = self.dstHost.hostname
        dst_datadir = self.target_datadir
        dst_port = self.target_port
        
        # Size information (if available)
        size_str = ""
        if self.segment_size:
            size_str = f" [{self.segment_size}]"

        return (
            f"Move Segment(content={self.seg.getSegmentContentId()}, dbid={self.seg.getSegmentDbId()}, "
            f"role={self.seg.role}){size_str}\n"
            f"      From: {src_host}:{src_port}:{src_datadir}\n"
            f"      To:   {dst_host}:{dst_port}:{dst_datadir}"
        )
    

class Plan:
    def __init__(self):
        self.target_segment_count = 0
        self.moves = None

    def getTargetSegmentCount(self) -> int:
        return self.target_segment_count

    def setTargetSegmentCount(self, target_segment_count: int) -> None:
        self.target_segment_count = target_segment_count
    
    def getMoves(self)-> List[LogicalMove]:
        return self.moves
    
    def setMoves(self, moves: List[LogicalMove]):
        self.moves = moves

    def serializePlan(self) -> bytes:
        return pickle.dumps(self)
    
    def __str__(self):
        lines = []

        lines.append(f"\n{'BALANCE MOVES':-^80}")
        if self.moves:
            lines.append(f"Total moves planned: {len(self.moves)}")
            lines.append("")
            
            for idx, move in enumerate(self.moves, 1):
                lines.append(f"  [{idx}] {move}")
                if idx < len(self.moves):
                    lines.append("")
        else:
            lines.append("  No data migration needed.")
            
        
        return "\n".join(lines)

def deserializePlan(input: bytes) -> Plan:
    return pickle.loads(input)

class ShrinkPlan(Plan):
    """
    Shrink plan
    """
    def __init__(self, shrinked_segs: List[gparray.SegmentPair]):
        Plan.__init__(self)
        self.shrinked_segments = shrinked_segs
    
    def __str__(self):
        """
        Pretty print shrink plan with segments to remove and moves to execute
        """
        lines = []
        lines.append("=" * 80)
        lines.append("SHRINK PLAN".center(80))
        lines.append("=" * 80)
        
        # Target segment count
        lines.append(f"\nTarget Segment Count: {self.target_segment_count}")
        
        # Shrinked segments section
        lines.append(f"\n{'SEGMENTS TO REMOVE':-^80}")
        if self.shrinked_segments:
            lines.append(f"Total segments to shrink: {len(self.shrinked_segments)}")
            lines.append("")
            
            for idx, seg_pair in enumerate(self.shrinked_segments, 1):
                lines.append(f"  [{idx}] Segment Pair:")
                
                # Primary segment
                if seg_pair.primaryDB:
                    lines.append(f"      Primary:")
                    lines.append(f"        Content:  {seg_pair.primaryDB.content}")
                    lines.append(f"        DbId:     {seg_pair.primaryDB.dbid}")
                    lines.append(f"        Host:     {seg_pair.primaryDB.hostname}")
                    lines.append(f"        Datadir:  {seg_pair.primaryDB.datadir}")
                    lines.append(f"        Port:     {seg_pair.primaryDB.port}")
                
                # Mirror segment
                if seg_pair.mirrorDB:
                    lines.append(f"      Mirror:")
                    lines.append(f"        Content:  {seg_pair.mirrorDB.content}")
                    lines.append(f"        DbId:     {seg_pair.mirrorDB.dbid}")
                    lines.append(f"        Host:     {seg_pair.mirrorDB.hostname}")
                    lines.append(f"        Datadir:  {seg_pair.mirrorDB.datadir}")
                    lines.append(f"        Port:     {seg_pair.mirrorDB.port}")
                
                if idx < len(self.shrinked_segments):
                    lines.append("")
        else:
            lines.append("  No segments to remove.")
        
        # Moves section
        lines.append(super().__str__())
        
        lines.append("\n" + "=" * 80)
        
        return "\n".join(lines)
    
class ConfigurationEncoder:

    @staticmethod
    def encode_configuration(gparray: gparray.GpArray, 
                             target_hosts: List[Host],
                             strategy: str,
                             resolver: HostResolver = HostResolver()) -> Tuple[SolverConfig, HostMapping]:
        """
        The rebalance solvers work with abstract input segment configuration.
        Each host must have an id in [0, n-1] interval, where n is a size
        of full hosts set, including decommissioned and new hosts. We encode
        segment placement as a vecor (h0, h1, ..., hj, ... hk), k - number of segments,
        hj - host id, j - segment contentid. E.x. H0 = {p0, p1, p2, m3, m4, m5}
        H1= {p3, p4, p5. m0, m1, m2}. We want to decommission H1 and add 2 new hosts H2
        and H3. Thus, the configuration will be encoded as:
        primaries (0, 0, 0, 3, 3, 3) mirrors (3, 3, 3, 0, 0, 0). Host id equal 3 corresponds
        to H1, since we want the decommissioned hosts be out of interval [0, nt-1] nt <= n,
        where nt - number of target hosts, which the rebalance should be performed on.
        The initial host load can encoded as (6, 0, 0, 6). n = 4, nt = 3.
                                              H0 H2 H3 H1
        After rebalance we want to get the configuration such that the load (4, 4, 4, 0)
        and mirroring strategy are achieved.
        """
        host_mapping = {}
        sorted_hosts = sorted(target_hosts, key=lambda h: h.status)
        for i, h in enumerate(sorted_hosts):
            host_mapping[h] = i
        primary_plcmnt = [0] * gparray.get_primary_count()
        mirror_plcmnt = [0] * gparray.get_primary_count()
        for pair in gparray.segmentPairs:
            primary_plcmnt[pair.primaryDB.content] = host_mapping[Host(hostname=pair.primaryDB.hostname,
                                                                       address=resolver.get_address(pair.primaryDB.hostname))]
            mirror_plcmnt[pair.mirrorDB.content] = host_mapping[Host(hostname=pair.mirrorDB.hostname,
                                                                     address=resolver.get_address(pair.mirrorDB.hostname))]
        n_initial = len(target_hosts)
        n_target = sum([1 for h in target_hosts if h.status != HostStatus.DECOMMISSIONED])
        conf = SolverConfig(gparray.get_primary_count(),
                            n_target,
                            n_initial,
                            primary_plcmnt,
                            mirror_plcmnt,
                            strategy)
        return (conf, host_mapping)

class PortAllocator:
    """
    Manages port allocation for segment moves

    Tracks existing ports and allocates new ones following the existing pattern
    """
    
    def __init__(self, gparray: gparray.GpArray, logger: Any,
                 verify_ports: bool = False):
        """
        Initialize port allocator with existing segment information
        
        Args:
            gparray: Current gp_segment_configuration
        """
        # Map: hostname -> set of ports currently in use
        self.existing_ports_by_host: Dict[str, Set[int]] = defaultdict(set)
        
        # Map: hostname -> (primary_base_port, mirror_base_port)
        self.base_ports_by_host: Dict[str, Tuple[int, int]] = {}
        
        # Map: hostname -> set of ports planned to be used (for moves)
        self.planned_ports_by_host: Dict[str, Set[int]] = defaultdict(set)

        # Map: hostname -> (set of primary ports, set of mirror ports)
        self.existing_ports_by_role: Dict[str, Tuple[Set[int], Set[int]]] = defaultdict(
            lambda: (set(), set())
        )

        self.logger = logger
        self.verify_ports = verify_ports
        
        # Initialize from existing segments
        self._initialize_from_array(gparray)
    
    def _initialize_from_array(self, gparray: gparray.GpArray):
        """
        Build initial port usage maps from gparray
        Tracks separate port patterns for primaries and mirrors
        """
        # First pass: collect all ports by role
        for seg in gparray.getSegDbList():
            hostname = seg.getSegmentHostName()
            port = seg.getSegmentPort()
            
            self.existing_ports_by_host[hostname].add(port)
            
            # Track ports by role (primary vs mirror)
            primary_ports, mirror_ports = self.existing_ports_by_role[hostname]
            if seg.isSegmentPrimary():
                primary_ports.add(port)
            else:
                mirror_ports.add(port)
        
        # Second pass: determine base ports for each role
        for hostname in self.existing_ports_by_role.keys():
            primary_ports, mirror_ports = self.existing_ports_by_role[hostname]
            
            primary_base = min(primary_ports) if primary_ports else None
            mirror_base = min(mirror_ports) if mirror_ports else None
            
            if primary_base is not None or mirror_base is not None:
                self.base_ports_by_host[hostname] = (primary_base, mirror_base)
    
    def allocate_port(self, 
                      host: Host, 
                      current_port: int,
                      is_mirror: bool) -> int:
        """
        Allocate a port for a segment being moved to target host
        
        Args:
            host: Target host
            current_port: Current port of segment on source host
            is_mirror: True if segment is a mirror, False if primary
            host_status: Status of target host (NEW or ACTIVE)
        
        Returns:
            Port number to use on target host
        """
        if host.status == HostStatus.NEW:
            return self._allocate_port_new_host(host, current_port, is_mirror)
        else:
            return self._allocate_port_existing_host(host, current_port, is_mirror)
    
    def _allocate_port_new_host(self, host: Host, current_port: int, is_mirror: bool) -> int:
        """
        Allocate port on a new host
        
        For new hosts, first primary/mirror establishes base port for that role.
        Subsequent segments of same role follow the established pattern.
        """
        primary_base, mirror_base = self.base_ports_by_host.get(host.hostname, (None, None))
        
        # If this is the first segment of this role on new host
        if is_mirror and mirror_base is None:
            # Establish mirror base port
            port = self._verify_and_allocate_port(host, current_port)
            self.base_ports_by_host[host.hostname] = (primary_base, port)
            self.planned_ports_by_host[host.hostname].add(port)
            # Track by role
            _, mirror_ports = self.existing_ports_by_role[host.hostname]
            mirror_ports.add(port)
            return port
        elif not is_mirror and primary_base is None:
            # Establish primary base port
            port = self._verify_and_allocate_port(host, current_port)
            self.base_ports_by_host[host.hostname] = (port, mirror_base)
            self.planned_ports_by_host[host.hostname].add(port)
            # Track by role
            primary_ports, _ = self.existing_ports_by_role[host.hostname]
            primary_ports.add(port)
            return port
        
        # Base port for this role already established, find next available
        return self._find_next_available_port(host, current_port, is_mirror)
    
    def _allocate_port_existing_host(self, host: Host, current_port: int, is_mirror: bool) -> int:
        """
        Allocate port on an existing host
        
        Try to use segment's current port if available, otherwise find next free port
        following the pattern for this role (primary/mirror)
        """
        # Try to use the segment's current port if it's free
        if self._is_port_available(host.hostname, current_port):
            if self.verify_ports:
                if not self._check_port_on_host(host, current_port):
                    self.logger.info(f"Port {current_port} on {host.hostname} "
                                     "appears in use, finding alternative")
                    return self._find_next_available_port(host, current_port, is_mirror)
            self.planned_ports_by_host[host.hostname].add(current_port)
            primary_ports, mirror_ports = self.existing_ports_by_role[host.hostname]
            if is_mirror:
                mirror_ports.add(current_port)
            else:
                primary_ports.add(current_port)
            return current_port
        
        # Port conflict - find next available port for this role
        return self._find_next_available_port(host, current_port, is_mirror)
    
    def _is_port_available(self, hostname: str, port: int) -> bool:
        """
        Check if port is available (not used by existing or planned segments)
        """
        return (port not in self.existing_ports_by_host[hostname] and
                port not in self.planned_ports_by_host[hostname])
    
    def _check_port_on_host(self, host: Host, port: int) -> bool:
        """
        Verify port is actually available on the target host
        """
        if not self.verify_ports:
            return True
                
        try:
            cmd = PortIsAvailable(
                name=f'check port {port} on {host.hostname}',
                port=port,
                ctxt=REMOTE,
                remoteHost=host.address
            )
            cmd.run()
            
            is_available = cmd.is_port_available()
            
            if self.logger:
                status = "available" if is_available else "in use"
                self.logger.debug(f"Port {port} on {host.hostname}: {status}")
            
            return is_available
            
        except Exception as e:
            self.logger.warning(f"Failed to verify port {port} on {host.hostname}: {e}. Assuming available.")
            # On error, assume available
            return True
    
    def _verify_and_allocate_port(self, host: Host, preferred_port: int) -> int:
        """
        Verify port is available on host and allocate it
        
        If verification fails, find next available port.
        """
        if self._is_port_available(host.hostname, preferred_port):
            if self.verify_ports and not self._check_port_on_host(host, preferred_port):
                # Preferred port is actually in use, find alternative
                self.logger.info(f"Preferred port {preferred_port} on {host.hostname} "
                                 "is in use, searching for alternative")
                self.existing_ports_by_host[host.hostname].add(preferred_port)
                return self._find_verified_port(host, preferred_port + 1)
            return preferred_port
        
        # Port not available in tracking, search from next
        return self._find_verified_port(host, preferred_port + 1)
    
    def _find_verified_port(self, host: Host, start_port: int) -> int:
        """
        Find an available port starting from start_port, with optional verification
        """
        candidate_port = start_port
        max_attempts = 1000
        
        for _ in range(max_attempts):
            if self._is_port_available(host.hostname, candidate_port):
                # Port available in tracking
                if self.verify_ports:
                    # Verify on actual host
                    if self._check_port_on_host(host, candidate_port):
                        return candidate_port
                    else:
                        # Port in use on host, mark and continue
                        self.existing_ports_by_host[host.hostname].add(candidate_port)
                        candidate_port += 1
                        continue
                else:
                    # No verification, trust tracking
                    return candidate_port
            
            candidate_port += 1
        
        raise PlanningError(
            f"Cannot find available port on host {host.hostname} "
            f"after {max_attempts} attempts starting from {start_port}"
        )

    def _find_next_available_port(self, host: Host, preferred_port: int, is_mirror: bool) -> int:
        """
        Find next available port following the pattern for this role
        """
        # Get base port for this role on this host
        primary_base, mirror_base = self.base_ports_by_host.get(host.hostname, (None, None))
        
        if is_mirror:
            base_port = mirror_base if mirror_base is not None else preferred_port
        else:
            base_port = primary_base if primary_base is not None else preferred_port
        
        # If no base port established yet, use preferred and establish it
        if is_mirror and mirror_base is None:
            self.base_ports_by_host[host.hostname] = (primary_base, base_port)
        elif not is_mirror and primary_base is None:
            self.base_ports_by_host[host.hostname] = (base_port, mirror_base)
        
        # Search for available port starting from base
        candidate_port = self._find_verified_port(host, base_port)

        self.planned_ports_by_host[host.hostname].add(candidate_port)
        
        # Track by role
        primary_ports, mirror_ports = self.existing_ports_by_role[host.hostname]
        if is_mirror:
            mirror_ports.add(candidate_port)
        else:
            primary_ports.add(candidate_port)
        
        return candidate_port

class Planner:
    """
    Performs the main planning procedure. Decides the operations to be performed
    on given configuration.
    """
    def __init__(self, logger: Any, dburl: dbconn.DbURL, gpArray: gparray.GpArray, options: Any):
        self.dburl = dburl
        self.gparray = gpArray
        self.options = options
        self.logger = logger
        self.virtual_gparray = deepcopy(self.gparray)

        if not self.virtual_gparray.hasMirrors:
            raise ValidationError("Cluster has mirroring disabled. Can't proceed with rebalance")

        if self.options.target_hosts_file:
                self.options.target_hosts = get_hosts_from_file(self.options.target_hosts_file, "target-hosts-file")
        if self.options.add_hosts_file:
                self.options.add_hosts = get_hosts_from_file(self.options.add_hosts_file, "add-hosts-file")
        if self.options.remove_hosts_file:
                self.options.remove_hosts = get_hosts_from_file(self.options.remove_hosts_file, "remove-hosts-file")

        self.validate_options()

        self.dir_template_p, self.dir_template_m = self.get_datadirs()

        self.resolver = HostResolver()
        target, add, remove = self.resolve_hosts()

        self.target_hosts, self.host_set_changed = Planner.get_target_hosts(self.virtual_gparray,
                                                                            self.dir_template_p,
                                                                            self.dir_template_m,
                                                                            target, add, remove,
                                                                            self.resolver
                                                                            )
    
    def validate_options(self):
        # Provide basic validation of hostnames and addresses.
        validate_hosts_basic(self.options.target_hosts, "target-hosts")
        validate_hosts_basic(self.options.add_hosts, "add-hosts")
        validate_hosts_basic(self.options.remove_hosts, "remove-hosts")

    def resolve_hosts(self) -> Tuple[List[str], List[str], List[str]]:
        """
        Returns:
             List of hostnames from target_hosts, add_hosts,
             remove_hosts options
        """
        existing_hosts = set()
        for seg in self.gparray.getSegDbList():
            existing_hosts.add(seg.getSegmentHostName())
        
        existing_hostname_list = list(existing_hosts)
        target_hostname_list = []
        add_hostname_list = []
        remove_hostname_list = []

        def _parse_and_resolve_hosts(
            hosts_input: str,
            option_name: str,
            check_exists: bool = False,
            check_not_exists: bool = False) -> List[str]:
            """
            Parse comma-separated host list, resolve IPs to hostnames, and validate

            Returns:
                List of resolved hostnames
            """
            hosts_list = [h.strip() for h in hosts_input.split(',')]
            resolved_hosts = []

            for host in hosts_list:
                # Resolve IP to hostname if needed
                if is_ip_address(host):
                    hostname = self.resolver.resolve_ip(host)
                    if not hostname:
                        raise ValidationError(f"{option_name}: Cannot resolve IP {host}")
                else:
                    hostname = host
                    self.resolver.resolve_hostname(hostname)

                # Check existence constraints
                if check_exists or check_not_exists:
                    matching_host = self.resolver.find_matching_hostname(hostname, existing_hostname_list)

                    if check_not_exists and matching_host:
                        raise ValidationError(
                            f"{option_name}: Host '{host}' already exists in cluster "
                            f"as '{matching_host}'"
                        )

                    if check_exists and not matching_host:
                        raise ValidationError(
                            f"{option_name}: Host '{host}' does not exist in cluster"
                        )

                resolved_hosts.append(hostname)

            return resolved_hosts

        if self.options.target_hosts:
            target_hostname_list = _parse_and_resolve_hosts(
                self.options.target_hosts, '--target-hosts')
        
        if self.options.add_hosts:
            add_hostname_list = _parse_and_resolve_hosts(
                self.options.add_hosts, '--add-hosts', check_not_exists=True)
        
        if self.options.remove_hosts:
            remove_hostname_list = _parse_and_resolve_hosts(
                self.options.remove_hosts, '--remove-hosts', check_exists=True)
        
        # Resolve the rest of hostnames
        for hostname in existing_hostname_list:
            self.resolver.resolve_hostname(hostname)
        
        return target_hostname_list, add_hostname_list, remove_hostname_list
                
    def plan(self) -> Plan:
        plan = Plan()

        self.validate_segment_status()
        if self.options.target_segment_count < self.gparray.get_segment_count():
            plan = self.plan_shrink()

        elif self.options.target_segment_count > self.gparray.get_segment_count():
            raise PlanningError("Expand is not supported yet")

        if self.options.skip_rebalance:
            self.logger.warning("Skipping rebalance")
            return plan

        rebalance_moves = self.form_moves()

        if rebalance_moves is None:
            self.logger.info("Cluster is already balanced, no segment moves will be held.")

        plan.setMoves(rebalance_moves)
        plan.setTargetSegmentCount(self.options.target_segment_count)
        return plan
    
    def validate_segment_status(self):
        if len([1 for pair in self.gparray.segmentPairs\
                if not pair.primaryDB.valid or not pair.mirrorDB.valid]):
            raise ValidationError("Some segments in 'down' status. ggrebalance can't proceed further")


    def plan_shrink(self) -> ShrinkPlan:
        self.logger.info("Planning shrink")
        shrinkedSegs = []
        for seg_pair in self.gparray.getSegmentList():
            primary_seg = seg_pair.primaryDB
            if primary_seg.getSegmentContentId() >= self.options.target_segment_count:
                shrinkedSegs.append(seg_pair)
                self.remove_segpair_from_array(seg_pair)
        plan = ShrinkPlan(shrinkedSegs)
        plan.setTargetSegmentCount(self.options.target_segment_count)
        return plan
    
    def remove_segpair_from_array(self, segPair: gparray.SegmentPair):
        """
        Removes the segment pair from gparray.
        """
        self.virtual_gparray.numPrimarySegments -= 1
        self.virtual_gparray.segmentPairs = list(filter(lambda x: x.primaryDB.dbid != segPair.primaryDB.dbid, 
                                                        self.virtual_gparray.segmentPairs))
        segs_list = self.virtual_gparray.getSegmentsAsLoadedFromDb()
        if segs_list:
            segs_list = list(filter(lambda x: x.dbid != segPair.primaryDB.dbid, 
                             segs_list))
            if segPair.mirrorDB:
                segs_list = list(filter(lambda x: x.dbid != segPair.mirrorDB.dbid, 
                             segs_list))
        self.virtual_gparray.setSegmentsAsLoadedFromDb(segs_list)

    @staticmethod
    def get_target_hosts(array: gparray.GpArray,
                         primary_template: str = DEFAULT_PRIMARY_TEMPLATE,
                         mirror_template: str = DEFAULT_MIRROR_TEMPLATE,
                         target_hostname_list: List[str] = [],
                         add_hostname_list: List[str] = [],
                         remove_hostname_list: List[str] = [],
                         resolver: HostResolver = HostResolver()) -> Tuple[List[Host], bool]:
        """
        Form set of hosts where we need to rebalance segments on
        Returns:
        - List of Host objects with:
          * For existing hosts: templates + existing datadirs filled
          * For new hosts: only templates, existing datadirs empty
        - Boolean indicating if host set changed
        """        
        hosts = {}
        for seg in array.segmentPairs:
            if seg.primaryDB.content >= 0:
                primary_host = seg.primaryDB.hostname
                mirror_host = seg.mirrorDB.hostname
                if primary_host not in hosts:
                    hosts[primary_host] = Host(hostname=primary_host,
                                               address=resolver.get_address(seg.primaryDB.hostname),
                                               datadir_info=DatadirInfo(primary_template, mirror_template),
                                               status = HostStatus.ACTIVE)
                if mirror_host not in hosts:
                    hosts[mirror_host] = Host(hostname=mirror_host,
                                               address=resolver.get_address(seg.mirrorDB.hostname),
                                               datadir_info=DatadirInfo(primary_template, mirror_template),
                                               status = HostStatus.ACTIVE)
        for pair in array.segmentPairs:
            primary = pair.primaryDB
            mirror = pair.mirrorDB
            primary_base = TemplateParser.extract_parent_directory(primary.datadir)
            hosts[primary.hostname].datadir_info.existing_primary_datadirs.add(primary_base)
            if mirror:
                mirror_base = TemplateParser.extract_parent_directory(mirror.datadir)
                hosts[mirror.hostname].datadir_info.existing_mirror_datadirs.add(mirror_base)

        host_set_changed = False
        if target_hostname_list:
            for host in hosts.keys():
                if host not in target_hostname_list:
                    hosts[host].status = HostStatus.DECOMMISSIONED
                    host_set_changed = True
            for host in target_hostname_list:
                if host not in hosts:
                    hosts[host] = Host(hostname=host,
                                       address=resolver.get_address(host),
                                       datadir_info=DatadirInfo(primary_template, mirror_template),
                                       status = HostStatus.NEW)
                    host_set_changed = True

        if add_hostname_list:
            for host in add_hostname_list:
                if host not in hosts:
                    hosts[host] = Host(hostname=host,
                                       address=resolver.get_address(host),
                                       datadir_info=DatadirInfo(primary_template, mirror_template),
                                       status = HostStatus.NEW)
                    host_set_changed = True

        if remove_hostname_list:
            for host in hosts.keys():
                if host in remove_hostname_list:
                    hosts[host].status = HostStatus.DECOMMISSIONED
                    host_set_changed = True

        return list(hosts.values()), host_set_changed
    
    def get_datadirs(self) -> Tuple[str, str]:
        """
        Get datadir templates from options or use defaults
        """
        if self.options.target_datadirs:
            return TemplateParser.parse_datadirs_input(self.options.target_datadirs)
        elif self.options.target_datadirs_file:
            return TemplateParser.parse_datadirs_file(self.options.target_datadirs_file)
        else:
            return DEFAULT_PRIMARY_TEMPLATE, DEFAULT_MIRROR_TEMPLATE

    def form_moves(self) -> List[LogicalMove]:
        self.logger.info("Validation of rebalance possibility")

        for pair in self.virtual_gparray.segmentPairs:
            prim = pair.primaryDB
            mir = pair.mirrorDB
            if prim.role != prim.preferred_role and mir.role != mir.preferred_role:
                raise ValidationError("Current role does not match preferred role for several segments.")
        
        total_primaries = self.virtual_gparray.get_primary_count()
        total_hosts = len([h for h in self.target_hosts\
                           if h.status == HostStatus.NEW or h.status == HostStatus.ACTIVE])
        if total_hosts < 2:
            raise ValidationError("Cannot perform rebalance at 1 host")
        if total_primaries % total_hosts != 0:
            raise ValidationError(f"Cannot evenly distribute {total_primaries}"
                                  f" segments across {total_hosts} hosts.")

        expected_per_host = total_primaries // total_hosts
        strat = self.options.mirror_mode

        if not strat:
            strat = GROUPED
        
        if strat == SPREAD and expected_per_host > total_hosts - 1:
            raise ValidationError("Cannot provide spread mirroring. Specify other "
                                  "mirroring strategy via -m option")

        if not self.host_set_changed\
            and self.already_balanced(expected_per_host):
            return None
        
        # dry movements are planned here
        config, host_mapping = ConfigurationEncoder.encode_configuration(self.virtual_gparray,
                                                                         self.target_hosts,
                                                                         strat,
                                                                         self.resolver)
        id_to_host = {v: k for k, v in host_mapping.items()}
        self.logger.info("Planning rebalance moves. Can take up to 60s.")
        if self.options.solver_seed:
            planning_seed_value = self.options.solver_seed
        else:
            planning_seed_value = int.from_bytes(os.urandom(16) , 'big')
        self.logger.info(f"Running randomized plan improvement with seed:{planning_seed_value}")
        lns_config = LNSConfig.from_parent(config, MAX_SOLVER_ITERS, SOLVER_TIMEOUT_SEC)
        solution, _ = LNS(lns_config, seed=planning_seed_value).solve()

        self.logger.debug(f"Solution: {solution}")
        self.logger.debug(f"Hosts: {id_to_host}")

        port_allocator = PortAllocator(self.virtual_gparray,
                                       self.logger,
                                       not self.options.skip_resource_estimation)
        final_moves = []
        moves = []
        for seg in self.virtual_gparray.getSegDbList():
            is_mirror = seg.isSegmentMirror()
            if is_mirror:
                hostId = config.initial_mirror_mapping[seg.content]
                final_hostId = solution[seg.content][1]
            else:
                hostId = config.initial_primary_mapping[seg.content]
                final_hostId = solution[seg.content][0]

            if hostId != final_hostId:
                #segment is moved
                source_host = id_to_host[hostId]
                target_host = id_to_host[final_hostId]
                target_datadir = self._get_datadir_for_segment(target_host, seg.content, is_mirror)

                # Allocate port for this segment on target host
                target_port = port_allocator.allocate_port(
                    host=target_host,
                    current_port=seg.getSegmentPort(),
                    is_mirror=is_mirror)

                moves.append(LogicalMove(seg, source_host, target_host, target_datadir, target_port))

        
        if len(moves) == 0:
            return None
        
        resource_estimator = None

        if not self.options.skip_resource_estimation:
            try:
                resource_estimator = ResourceEstimator(
                        self.logger, 
                        self.dburl, 
                        self.virtual_gparray,
                        batch_size=getattr(self.options, 'batch_size', 16)
                        )
                resource_estimator.estimate_and_validate_moves(moves)
            except ResourceError as e:
                    raise PlanningError(f"Resource validation failed: {e}")
        else:
            self.logger.warning("Skipping resource estimation")
        # Detect swaps and mirror-primary conflicts
        swap_pairs: List[Tuple[LogicalMove, LogicalMove]] = []
        conflict_pairs: List[Tuple[LogicalMove, LogicalMove]] = []
        swap_dbids: Set[int] = set()

        if not self.options.inplace_swap_roles:
            swap_pairs = self.detect_swap_pairs(moves)
            if swap_pairs:
                self.logger.info(f"Detected {len(swap_pairs)} primary-mirror pairs that swap hosts")
                swap_dbids = {dbid for p, m in swap_pairs for dbid in (p.seg.getSegmentDbId(), m.seg.getSegmentDbId())}
            conflict_pairs = self.detect_conflict_pairs(moves, swap_dbids)
            if conflict_pairs:
                self.logger.info(f"Detected {len(conflict_pairs)} mirror-primary ordering conflicts")

        if swap_pairs and conflict_pairs:
            self.logger.info(f"Detected {len(swap_pairs)} primary-mirror pairs which just swap hosts")
            phase1, phase2, phase3, handled_dbids = self.decompose_swap_pairs(swap_pairs + conflict_pairs,
                                                                              port_allocator,
                                                                              resource_estimator)
            final_moves = self._group_swap_moves(moves, handled_dbids, phase1, phase2, phase3)
        elif swap_pairs:
            phase1, phase2, phase3, handled_dbids = self.decompose_swap_pairs(swap_pairs,
                                                                              port_allocator,
                                                                              resource_estimator)
            final_moves = self._group_swap_moves(moves, handled_dbids, phase1, phase2, phase3)
        elif conflict_pairs:
            # no intermediate host needed
            final_moves = self._group_conflict_moves(moves, conflict_pairs)
        else:
            final_moves = self._group_plain_moves(moves)

        if resource_estimator:
            self.logger.info(
                f"Estimated total data to move: "
                f"{resource_estimator.total_gb(final_moves):.2f} GB"
            )

        return final_moves
    
    def already_balanced(self, load: int) -> bool:
        primaries_by_host = defaultdict(int)
        mirrors_by_host = defaultdict(int)
        for pair in self.virtual_gparray.segmentPairs:
            primaries_by_host[pair.primaryDB.hostname] += 1
            if pair.mirrorDB:
                mirrors_by_host[pair.mirrorDB.hostname] += 1
                # This is mirroring strategy violation
                if pair.mirrorDB.hostname == pair.primaryDB.hostname:
                    return False
        for n in primaries_by_host.values():
            if n != load:
                return False
        for n in mirrors_by_host.values():
            if n != load:
                return False
        return True
    
    def _get_datadir_for_segment(self, host: Host, content_id: int, is_mirror: bool) -> str:
        """
        Determine datadir for a segment on target host
        
        Logic:
        - Always use template for either new/exising hosts
        """
        datadir_info = host.datadir_info
        
        assert(host.status == HostStatus.NEW or host.status == HostStatus.ACTIVE)
        # use template
        template = datadir_info.mirror_template if is_mirror else datadir_info.primary_template
        return TemplateParser.instantiate_template(template, host.hostname, content_id)
    
    def detect_swap_pairs(self, moves: List[LogicalMove]) -> List[Tuple[LogicalMove, LogicalMove]]:
        """
        Detect primary-mirror swap scenarios
        
        A swap occurs when:
        - Primary of content N moves from host A to host B
        - Mirror of content N moves from host B to host A
        
        Returns:
            List of (primary_move, mirror_move) tuples for detected swaps
        """
        # Index moves by content
        moves_by_content = defaultdict(dict)
        for move in moves:
            content_id = move.seg.getSegmentContentId()
            if move.seg.isSegmentPrimary():
                moves_by_content[content_id]['primary'] = move
            else:
                moves_by_content[content_id]['mirror'] = move
        
        swap_pairs = []
        for content_id, seg_moves in moves_by_content.items():
            # Check if both primary and mirror are moved
            if 'primary' not in seg_moves or 'mirror' not in seg_moves:
                continue
            
            prim_move = seg_moves['primary']
            mir_move = seg_moves['mirror']
            
            # Check if they're swapping hosts
            prim_src = prim_move.seg.getSegmentHostName()
            prim_dst = prim_move.dstHost.hostname
            mir_src = mir_move.seg.getSegmentHostName()
            mir_dst = mir_move.dstHost.hostname
            
            if (prim_src == mir_dst and 
                prim_dst == mir_src):
                swap_pairs.append((prim_move, mir_move))
                self.logger.debug(
                    f"Detected swap for content {content_id}: "
                    f"{prim_src} <-> {prim_dst}"
                )
        
        return swap_pairs
    
    def _group_plain_moves(self, moves: List[LogicalMove]) -> List[LogicalMove]:
        """
        No-swap case: group mirror moves before primary moves.
        Produces exactly one switchover pair in the executor.

        Order: [ mirrors ... | primaries ... ]
        """
        mirrors  = [m for m in moves if m.seg.isSegmentMirror()]
        primaries = [m for m in moves if not m.seg.isSegmentMirror()]
        return mirrors + primaries

    def _group_swap_moves(self,
                          all_moves: List[LogicalMove],
                          swap_dbids: Set[int],
                          phase1: List[LogicalMove],
                          phase2: List[LogicalMove],
                          phase3: List[LogicalMove],
                          ) -> List[LogicalMove]:
        """
        Swap case: put non-swap moves into the correct phase groups.
        Produces exactly one switchover pair.

        Order:
            [ non_swap_mirrors + phase1 ] - all mirror moves, no switchover
            [ non_swap_primaries + phase2 ] - all primary moves, one switchover pair
            [ phase3 ] - residual swap mirrors, no switchover
        """
        non_swap_mirrors: List[LogicalMove] = []
        non_swap_primaries: List[LogicalMove] = []

        for move in all_moves:
            if move.seg.getSegmentDbId() in swap_dbids:
                continue
            if move.seg.isSegmentMirror():
                non_swap_mirrors.append(move)
            else:
                non_swap_primaries.append(move)

        return (
            non_swap_mirrors + phase1 +
            non_swap_primaries + phase2 +
            phase3
        )

    def _group_conflict_moves(self,
                              moves: List[LogicalMove],
                              conflict_pairs: List[Tuple[LogicalMove, LogicalMove]]
                              ) -> List[LogicalMove]:
        """
        One-way conflict: delay conflicting mirrors until after all primaries
        have vacated their current hosts.

        The conflicting mirror wants to land on a host that currently holds
        a primary (mir_dst == prim_src).  We hold the mirror back
        until the primary has moved away.

        Executor pattern: identical to the phase-3 tail in _group_swap_moves.

        Order:
            [ non-conflict mirrors ] pre-switchover, no co-location risk
            [ all primaries        ] post-switchover; vacate conflict hosts
            [ conflict mirrors     ] conflict hosts now free, safe to land
        """
        conflict_mirror_dbids: Set[int] = {
            m.seg.getSegmentDbId() for _, m in conflict_pairs
        }

        non_conflict_mirrors: List[LogicalMove] = []
        conflict_mirrors:     List[LogicalMove] = []
        primaries:            List[LogicalMove] = []

        for move in moves:
            if move.seg.isSegmentMirror():
                if move.seg.getSegmentDbId() in conflict_mirror_dbids:
                    conflict_mirrors.append(move)
                else:
                    non_conflict_mirrors.append(move)
            else:
                primaries.append(move)

        return non_conflict_mirrors + primaries + conflict_mirrors

    def select_intermediate_host(self, 
                                primary_move: LogicalMove,
                                mirror_move: LogicalMove,
                                used_intermediate_hosts: Dict[str, int],
                                resource_estimator: Optional['ResourceEstimator'] = None
                                 ) -> Host:
        """
        Select an intermediate host for swap operation with basic space validation
        
        Naive criteria (since the executor runs all mirror moves before role swap):
        1. Not involved in this swap (not source or dest for either segment)
        2. Has sufficient space for the mirror PLUS other planned moves to that host
        3. Prefer hosts with fewer intermediate segments already assigned
        
        Args:
            primary_move: Primary segment move
            mirror_move: Mirror segment move
            used_intermediate_hosts: Dict tracking hostname -> count of intermediates assigned
            resource_estimator: Optional ResourceEstimator for space validation
        
        Returns:
            Selected intermediate host
        """
        content_id = primary_move.seg.getSegmentContentId()
        
        # Collect excluded hostnames
        excluded_hosts = {
            primary_move.dstHost.hostname,
            mirror_move.dstHost.hostname
        }
        
        # Find candidate hosts
        candidates = []
        for host in self.target_hosts:
            if host.status not in [HostStatus.ACTIVE, HostStatus.NEW]:
                continue
            
            # Check if excluded
            if host.hostname in excluded_hosts:
                continue
            
            candidates.append(host)
        
        if not candidates:
            raise PlanningError(
                f"No intermediate host available for swap of content {content_id}. "
                f"Need at least 3 hosts to perform swaps safely. Or you may allow "
                f"primary-mirror coexistence by --inplace-swap-roles."
            )
        
        # Get mirror size for space check
        mirror_size_kb = 0
        if mirror_move.segment_size:
            mirror_size_kb = int(mirror_move.segment_size.total_size_kb * (1 + DISK_SPACE_SAFETY_MARGIN))

        # Score each candidate
        scored_candidates = []
        
        for host in candidates:
            # Check space if resource estimation is enabled
            has_space = True
            available_space_kb = float('inf')
            filesystems = 'unknown'
            
            if resource_estimator:
                try:
                    intermediate_datadir = self._get_datadir_for_segment(
                        host, content_id, is_mirror=True)

                    has_space, available_space_kb, filesystems = \
                        resource_estimator.check_space(
                            hostname=host.hostname,
                            host_address=host.address,
                            data_directory=intermediate_datadir,
                            required_size=mirror_move.segment_size
                            )

                    if not has_space:
                        self.logger.debug(
                            f"Host {host.hostname}: insufficient space insufficient space on {filesystems}\n"
                            f"  Directory: {intermediate_datadir}\n"
                            f"  (available: {available_space_kb / 1024 / 1024:.2f} GB, "
                            f"  required: {mirror_size_kb / 1024 / 1024:.2f} GB)"
                        )
                        continue
                    else:
                        self.logger.debug(
                            f"Host {host.hostname}: viable candidate\n"
                            f"  Directory: {intermediate_datadir}\n"
                            f"  Filesystem: {filesystems}\n"
                            f"  Available: {available_space_kb / 1024 / 1024:.2f} GB\n"
                            f"  Required: {mirror_size_kb / 1024 / 1024:.2f} GB"
                        )
                except Exception as e:
                    self.logger.error(f"Could not check space on {host.hostname}: {e}")
                    has_space = False
            
            if not has_space:
                continue
            
            # Calculate score
            # Factors: available space, intermediate usage
            if mirror_size_kb <= 0:
                space_score = 1.0
            else:
                ratio = available_space_kb / mirror_size_kb
                # sigmoid. more sensitive around ratio=1
                space_score = 1.0 / (1.0 + math.exp(-3.0 * (ratio - 1.0)))
            
            # Penalize hosts already used as intermediate
            intermediate_count = used_intermediate_hosts.get(host.hostname, 0)
            intermediate_score = math.exp(-1.0 * intermediate_count)
            
            # make score lie in [0, 1]
            score = space_score * 0.7 + intermediate_score * 0.3
            
            scored_candidates.append((host, score, available_space_kb, filesystems))
        
        if not scored_candidates:
            raise PlanningError(
                f"No suitable intermediate host found for swap of content {content_id}. "
                f"Need at least 3 hosts with sufficient space to perform swaps safely."
            )
        
        # Sort by score (higher is better)
        scored_candidates.sort(key=lambda x: x[1], reverse=True)
        
        selected_host, selected_score, selected_space, selected_fs = scored_candidates[0]
        
        # Track usage
        used_intermediate_hosts[selected_host.hostname] = \
            used_intermediate_hosts.get(selected_host.hostname, 0) + 1
        
        self.logger.info(
            f"Selected intermediate host {selected_host.hostname} for content {content_id}\n"
            f"  Score: {selected_score:.1f}\n"
            f"  Filesystems: {selected_fs}\n"
            f"  Available: {selected_space / 1024 / 1024:.2f} GB\n"
            f"  Required: {mirror_size_kb / 1024 / 1024:.2f} GB")
        
        if resource_estimator:
            resource_estimator.reserve_space(
                hostname=host.hostname,
                host_address=host.address,
                data_directory=self._get_datadir_for_segment(host, content_id, is_mirror=True),
                dbid=mirror_move.seg.dbid,
                required_size=mirror_move.segment_size
            )
        
        return selected_host
    
    def decompose_swap(self,
                      primary_move: LogicalMove,
                      mirror_move: LogicalMove,
                      intermediate_host: Host,
                      port_allocator: PortAllocator) -> Tuple[LogicalMove, LogicalMove, LogicalMove]:
        """
        Decompose a swap into 3 moves through intermediate host
        
        Phase 1: Mirror -> Intermediate host
        Phase 2: Primary -> Mirror's original host (direct move)
        Phase 3: Mirror (from intermediate) -> Primary's original host
        
        Returns:
            (phase1_move, phase2_move, phase3_move)
        """
        content_id = primary_move.seg.getSegmentContentId()
        
        # Phase 1: Move mirror to intermediate host
        intermediate_datadir = self._get_datadir_for_segment(
            intermediate_host,
            content_id,
            is_mirror=True
        )
        intermediate_port = port_allocator.allocate_port(
            host=intermediate_host,
            current_port=mirror_move.seg.getSegmentPort(),
            is_mirror=True
        )
        
        phase1_move = LogicalMove(
            seg=mirror_move.seg,
            srcHost=mirror_move.srcHost,
            dstHost=intermediate_host,
            target_datadir=intermediate_datadir,
            target_port=intermediate_port,
            segment_size=mirror_move.segment_size,
            swap_metadata={
                'phase': 1,
                'content_id': content_id,
                'intermediate_host': intermediate_host.hostname
            }
        )
        
        # Phase 2: Primary move (direct to final location)
        # This keeps the original primary move unchanged
        phase2_move = primary_move
        phase2_move.swap_metadata = {'phase': 2,
                                    'content_id': content_id}
        
        # Phase 3: Move mirror from intermediate to final location (primary's original host)
        final_datadir = mirror_move.target_datadir
        final_port = port_allocator.allocate_port(
            host=mirror_move.dstHost,
            current_port=mirror_move.seg.getSegmentPort(),
            is_mirror=True
        )
        
        # Create a copy of mirror segment with updated location (now on intermediate)
        intermediate_seg = copy.deepcopy(mirror_move.seg)
        intermediate_seg.hostname = intermediate_host.hostname
        intermediate_seg.address = intermediate_host.address
        intermediate_seg.datadir = intermediate_datadir
        intermediate_seg.port = intermediate_port
        
        phase3_move = LogicalMove(
            seg=intermediate_seg,
            srcHost=intermediate_host,
            dstHost=mirror_move.dstHost,  # Final location (primary's original host)
            target_datadir=final_datadir,
            target_port=final_port,
            segment_size=mirror_move.segment_size,
            swap_metadata={
                'phase': 3,
                'content_id': content_id,
                'intermediate_host': intermediate_host.hostname
            }
        )
        
        return phase1_move, phase2_move, phase3_move
    
    def decompose_swap_pairs(self,
                     swap_pairs: List[Tuple[LogicalMove, LogicalMove]],
                     port_allocator: PortAllocator,
                     resource_estimator: Optional['ResourceEstimator']
                     ) -> Tuple[List[LogicalMove], List[LogicalMove], List[LogicalMove], Set[int]]:
        """
        swap pairs - 3 phases.

        Returns:
            phase1_mirrors - mirror moves to intermediate host
            phase2_primaries - primary direct moves
            phase3_mirrors - mirrors from intermediate to final host
            swap_dbids - dbids used in swaps
        """
        swap_dbids: Set[int] = set()
        phase1, phase2, phase3 = [], [], []
        used_intermediate_hosts: Dict[str, int] = {}

        for prim_move, mir_move in swap_pairs:
            swap_dbids.add(prim_move.seg.getSegmentDbId())
            swap_dbids.add(mir_move.seg.getSegmentDbId())
            try:
		# Select intermediate host
                intermediate_host = self.select_intermediate_host(
                    prim_move,
                    mir_move,
                    used_intermediate_hosts,
                    resource_estimator # Pass the estimator with cached filesystem data
                )
                # Decompose into 3 phases
                p1, p2, p3 = self.decompose_swap(
                    prim_move,
                    mir_move,
                    intermediate_host,
                    port_allocator
                )
                phase1.append(p1)
                phase2.append(p2)
                phase3.append(p3)
            except Exception as e:
                raise PlanningError(f"Failed to plan swap for content {prim_move.seg.getSegmentContentId()}: {e}")

        return phase1, phase2, phase3, swap_dbids
    
    def detect_conflict_pairs(self,
                              moves: List[LogicalMove],
                              swap_dbids: Set[int]
                              ) -> List[Tuple[LogicalMove, LogicalMove]]:
        """
        Detect one-way host conflicts: mirror's destination == primary's current host,
        but the primary is not going back to mirror's host (that would be a true swap,
        already caught by detect_swap_pairs).

        Args:
            moves: all planned logical moves
            swap_dbids: dbids already claimed by swap detection

        Returns:
            List of (primary_move, mirror_move) conflict pairs
        """
        moves_by_content: Dict[int, Dict[str, LogicalMove]] = defaultdict(dict)
        for move in moves:
            if move.seg.getSegmentDbId() in swap_dbids:
                continue
            content_id = move.seg.getSegmentContentId()
            key = 'primary' if move.seg.isSegmentPrimary() else 'mirror'
            moves_by_content[content_id][key] = move

        conflict_pairs = []
        for content_id, seg_moves in moves_by_content.items():
            if 'primary' not in seg_moves or 'mirror' not in seg_moves:
                continue

            prim_move = seg_moves['primary']
            mir_move  = seg_moves['mirror']

            prim_src = prim_move.seg.getSegmentHostName()
            mir_dst  = mir_move.dstHost.hostname

            if mir_dst == prim_src:
                conflict_pairs.append((prim_move, mir_move))
                self.logger.debug(
                    f"Detected one-way conflict for content {content_id}: "
                    f"mirror -> {mir_dst} conflicts with primary currently on {prim_src}"
                )
            # prim_dst == mir_src is safe by default

        return conflict_pairs


@dataclass
class FilesystemRequirement:
    """
    Tracks space requirements and allocations for a single filesystem on a host.
    """
    space_info: DiskSpaceInfo
    required_kb: int = 0
    # dbids and datadirs, which contribute to this filesystem
    dbid_datadirs: Dict[int, Tuple[str, int]] = field(default_factory=dict)
    # dict(dbid, dict(tablespace_base, size_kb)) contributing to this filesystem
    dbid_tablespaces: Dict[int, Dict[str, int]] = field(default_factory=dict)
    # tablespace_base/dbid for error msgs
    tablespace_paths: Set[str] = field(default_factory=set)

    def add_datadir(self, dbid: int, directory: str, size_kb: int) -> None:
        """
        Add a datadir contribution.
        """
        request = int(size_kb * (1 + DISK_SPACE_SAFETY_MARGIN))
        self.dbid_datadirs[dbid] = (directory, request)
        self.required_kb += request

    def add_tablespace(self, dbid: int, tbl_path: str, tbl_base: str, size_kb: int) -> None:
        """
        Add a tablespace contribution.
        """
        request = int(size_kb * (1 + DISK_SPACE_SAFETY_MARGIN))
        self.tablespace_paths.add(tbl_path)
        if dbid not in self.dbid_tablespaces:
            self.dbid_tablespaces[dbid] = defaultdict(int)
        self.dbid_tablespaces[dbid][tbl_base] += request
        self.required_kb += request
    
    def remove_datadir(self, dbid: int) -> None:
        if dbid in self.dbid_datadirs:
            item = self.dbid_datadirs.pop(dbid)
            self.required_kb -= item[1]

    def remove_tablespaces(self, dbid: int) -> None:
        if dbid in self.dbid_tablespaces:
            tblsps = self.dbid_tablespaces.pop(dbid)
            for _, size in tblsps.items():
                self.required_kb -= size

    @property
    def available_kb(self) -> int:
        return self.space_info.available_kb
    
    @property
    def datadir_paths(self) -> Set[str]:
        """for error reporting"""
        paths = set()
        for _, item in self.dbid_datadirs.items():
            paths.add(item[0])
        return paths

    @property
    def unique_segment_count(self) -> int:
        return len(self.dbid_datadirs) + \
            len(set(self.dbid_tablespaces.keys()) - set(self.dbid_datadirs.keys()))

class ResourceEstimator:
    """
    Estimates and validates resource requirements for segment moves
    
    This class handles move-specific resource estimation logic,
    using DiskSpaceChecker for invoking remote disk operations.
    """
    
    def __init__(self, logger: Any, dburl: dbconn.DbURL, gparray: gparray.GpArray, batch_size: int = 16):
        """
        Initialize resource estimator
        
        Args:
            logger: Logger instance
            dburl: Database url
            gparray: Current GpArray configuration
            batch_size: Number of parallel operations
        """
        self.logger = logger
        self.dburl = dburl
        self.gparray = gparray
        self.disk_checker = DiskSpaceChecker(logger, batch_size)
        # Cache filesystem allocation data after validation
        # Maps (hostname, filesystem) -> {required_kb, available_kb, datadir_moves, ...}
        self.filesystem_allocations: Dict[Tuple[str, str], FilesystemRequirement] = {}
        # Cache space_info_by_host for reuse
        self.space_info_by_host: Dict[str, Dict[str, DiskSpaceInfo]] = {}
    
    def estimate_and_validate_moves(self, moves: List['LogicalMove']) -> None:
        """
        Estimate resource requirements and validate moves can be performed
        
        This method modifies the moves in-place, setting the segment_size attribute.
        
        Args:
            moves: List of LogicalMove objects to validate
        """
        if not moves:
            return
        
        self.logger.info(f"Estimating resource requirements for {len(moves)} segment moves...")
        # Step 1: Estimate segment sizes
        self._estimate_segment_sizes(moves)
        self.logger.info("Validating available disk space on target hosts...")
        # Step 2: Validate available space on target hosts
        self._validate_and_build_allocations(moves)
    
    def total_gb(self, moves: List[LogicalMove]) -> int:
        total_size_kb = sum(move.segment_size.total_size_kb for move in moves if move.segment_size)
        return total_size_kb / 1024 / 1024
    
    def _estimate_segment_sizes(self, moves: List[LogicalMove]) -> None:
        """
        Estimate sizes for all segments being moved
        
        Populates the segment_size attribute on each LogicalMove
        """
        tablespace_map = self._get_tablespace_locations([m.seg.dbid for m in moves])
        self._estimate_datadir_sizes(moves)
        if tablespace_map:
            self._estimate_tablespace_sizes(moves, tablespace_map)

    def _estimate_datadir_sizes(self, moves: List[LogicalMove]) -> None:
        moves_by_host: Dict[str, List[LogicalMove]] = defaultdict(list)
        for move in moves:
            moves_by_host[move.srcHost.address].append(move)
        
        for host_addr, host_moves in moves_by_host.items():
            dirs = [m.seg.datadir for m in host_moves]
            try:
                disk_usage = self.disk_checker.get_disk_usage(host_addr, dirs)
                for move in host_moves:
                    size_kb = disk_usage.get(move.seg.datadir, 0)
                    move.segment_size = SegmentSize(datadir_size_kb=size_kb)
            except Exception as e:
                raise ResourceError(f"Cannot estimate segment sizes on host {host_addr}: {e}")
    
    def _get_tablespace_locations(self, dbids: List[int]) -> Dict[int, List[str]]:
        """
        Query database for tablespace locations for given segments
        
        Returns:
            Dict mapping dbid to list of tablespace paths
        """

        if not dbids:
            return {}
        
        oid_subq = """
            (SELECT *
             FROM (
                 SELECT oid FROM pg_tablespace
                 WHERE spcname NOT IN ('pg_default', 'pg_global')
             ) AS _q1,
             LATERAL gp_tablespace_location(_q1.oid)
            ) AS t
        """
        
        segment_dbids = ','.join(f'({dbid})' for dbid in dbids)
        
        tablespace_location_sql = f"""
            SELECT c.dbid, t.tblspc_loc||'/'||c.dbid AS tblspc_loc
            FROM {oid_subq}
                JOIN gp_segment_configuration AS c
                ON t.gp_segment_id = c.content 
            WHERE c.dbid IN (VALUES {segment_dbids})
        """
        
        try:
            with closing(dbconn.connect(self.dburl, encoding='UTF8')) as conn:
                cursor = dbconn.query(conn, tablespace_location_sql)
                tablespaces = defaultdict(list)

                for dbid, loc in cursor:
                    tablespaces[dbid].append(loc)

                return tablespaces
        except Exception as e:
            raise ResourceError(f"Failed to query tablespace locations: {e}")
    
    def _estimate_tablespace_sizes(self, 
                                   moves: List['LogicalMove'],
                                   tablespace_map: Dict[int, List[str]]) -> None:
        """
        Estimate tablespace sizes and add to segment_size
        
        Modifies move.segment_size in-place
        """
        if not tablespace_map:
            return
        
        # Group tablespace directories by host
        tblspace_by_host = defaultdict(list)
        for move in moves:
            for tbl_dir in tablespace_map.get(move.seg.dbid, []):
                tblspace_by_host[move.srcHost.address].append((move.seg.dbid, tbl_dir))
        
        # Process each host
        for host_addr, host_tablespaces in tblspace_by_host.items():
            # Get unique directories
            dirs = list(set(tblspace_dir for _, tblspace_dir in host_tablespaces))
            
            try:
                disk_usage = self.disk_checker.get_disk_usage(host_addr, dirs)
                
                # Aggregate by segment
                for dbid, tblspace_dir in host_tablespaces:
                    size_kb = disk_usage.get(tblspace_dir, 0)
                    
                    # Find all moves for this segment and add tablespace size
                    for move in moves:
                        if move.seg.dbid == dbid and move.segment_size:
                            if move.segment_size.tablespace_usage is None:
                                move.segment_size.tablespace_usage = {}
                            move.segment_size.tablespace_usage[tblspace_dir] = size_kb
                        
            except Exception as e:
                self.logger.warning(f"Failed to get tablespace disk usage for host {host_addr}: {e}")
                # Continue without tablespace sizes
    
    def _validate_and_build_allocations(self, moves: List[LogicalMove]) -> None:
        """
        Check that every target filesystem has enough free space.

        Builds filesystem_allocations as a side effect so that
        reserve_space() can later account for committed space.
        Raises ResourceError listing all filesystems with insufficient space.
        """
        self._fetch_target_space_info(moves)
        self._build_filesystem_allocations(moves)
        issues = self._find_space_issues()

        if issues:
            details = ''.join(self._format_space_issue(i) for i in issues)
            raise ResourceError(
                f"Insufficient disk space for rebalance operation:\n{details}"
                f"\nNote: Estimates include {int(DISK_SPACE_SAFETY_MARGIN * 100)}% safety margin"
            )
        self.logger.info("Disk space validation completed successfully")
    
    def _fetch_target_space_info(self, moves: List[LogicalMove]) -> None:
        """
        Query free space for every target directory referenced by the moves.
        """
        dirs_by_host: Dict[str, Set[str]] = defaultdict(set)
        for move in moves:
            if not move.segment_size:
                continue
            dirs_by_host[move.dstHost.address].add(move.target_datadir)
            for tbl_path in (move.segment_size.tablespace_usage or {}):
                dirs_by_host[move.dstHost.address].add(os.path.dirname(tbl_path))

        try:
            self.space_info_by_host = self.disk_checker.check_batch_available_space(
                {host: list(dirs) for host, dirs in dirs_by_host.items()}
            )
        except Exception as e:
            raise ResourceError(f"Failed to check available disk space: {e}")
    
    def _build_filesystem_allocations(self, moves: List[LogicalMove]) -> None:
        """
        Populate self.filesystem_allocations from the planned moves.

        Each move contributes its datadir size to the filesystem that hosts
        target_datadir, and each tablespace path to the filesystem that hosts
        that tablespace.
        """
        self.filesystem_allocations = {}
        self.logger.debug("Aggregating space requirements by filesystem...")

        for move in moves:
            if not move.segment_size:
                continue

            hostname = move.dstHost.hostname
            host_address = move.dstHost.address

            # datadir
            datadir_fs = self._require_space_info(hostname, host_address, move.target_datadir)
            fs_req = self._get_or_create_fs_requirement(hostname, datadir_fs)
            fs_req.add_datadir(move.seg.dbid, move.target_datadir, move.segment_size.datadir_size_kb)

            # tablespace
            for tbl_path, size_kb in (move.segment_size.tablespace_usage or {}).items():
                tbl_base = os.path.dirname(tbl_path)
                tbl_fs = self._require_space_info(hostname, host_address, tbl_base)
                fs_req = self._get_or_create_fs_requirement(hostname, tbl_fs)
                fs_req.add_tablespace(move.seg.dbid, tbl_path, tbl_base, size_kb)

    def _find_space_issues(self) -> List[Dict]:
        issues = []
        for (hostname, filesystem), fs_req in self.filesystem_allocations.items():
            required_gb = fs_req.required_kb / 1024 / 1024
            available_gb = fs_req.available_kb / 1024 / 1024

            self.logger.debug(
                f"Filesystem {filesystem} on {hostname}: "
                f"required {required_gb:.2f} GB, available {available_gb:.2f} GB ")

            if fs_req.available_kb < fs_req.required_kb:
                issues.append({
                    'hostname': hostname,
                    'filesystem': filesystem,
                    'target_dirs': sorted(fs_req.datadir_paths | fs_req.tablespace_paths),
                    'num_datadirs': len(fs_req.datadir_paths),
                    'num_tablespaces': len(fs_req.tablespace_paths),
                    'num_segments': fs_req.unique_segment_count,
                    'required_gb': required_gb,
                    'available_gb': available_gb,
                })
        return issues

    def _get_or_create_fs_requirement(self, hostname: str, space_info: DiskSpaceInfo) -> FilesystemRequirement:
        fs_key = (hostname, space_info.filesystem)
        if fs_key not in self.filesystem_allocations:
            self.filesystem_allocations[fs_key] = FilesystemRequirement(space_info=space_info)
        return self.filesystem_allocations[fs_key]

    def _require_space_info(self, hostname: str, host_address: str, directory: str) -> DiskSpaceInfo:
        """
        Return DiskSpaceInfo for directory, raising ResourceError if not found.
        """
        info = self._get_or_fetch_space_info(hostname, host_address, directory)
        if not info:
            raise ResourceError(f"No disk space information for {hostname}:{directory}")
        return info

    def _get_or_fetch_space_info(self, hostname: str, host_address: str, directory: str) -> Optional[DiskSpaceInfo]:
        """
        Return cached DiskSpaceInfo for directory (or its parent).
        On a cache miss, query the host and cache the result.
        """
        # Check cache
        host_cache = self.space_info_by_host.get(host_address, {})
        info = host_cache.get(directory) or host_cache.get(
            TemplateParser.extract_parent_directory(directory)
        )
        if info:
            return info

        # Cache miss - query the host
        try:
            fetched = self.disk_checker.check_batch_available_space(
                {host_address: [directory]}
            )
            info = fetched.get(host_address, {}).get(directory)
            if info:
                self.space_info_by_host.setdefault(host_address, {})[directory] = info
            return info
        except Exception as e:
            self.logger.warning(f"Could not check space for {hostname}:{directory}: {e}")
            return None

    def _reserve_space(self,
                       fs_key: Tuple[str, str],
                       space_info: DiskSpaceInfo,
                       directory: str,
                       required_kb: int,
                       dbid: int,
                       is_tablespace: bool) -> None:
        hostname, filesystem = fs_key
        if fs_key not in self.filesystem_allocations:
            self.filesystem_allocations[fs_key] = FilesystemRequirement(space_info=space_info)
        if is_tablespace:
            self.filesystem_allocations[fs_key].add_tablespace(dbid, directory + '/' + str(dbid), directory, required_kb)
        else:
             self.filesystem_allocations[fs_key].add_datadir(dbid, directory, required_kb)
        total_allocated = self.filesystem_allocations[fs_key].required_kb
        self.logger.debug(
            f"Reserved {required_kb / 1024 / 1024:.2f} GB on {hostname}:{filesystem} - "
            f"total allocated: {total_allocated / 1024 / 1024:.2f} GB"
        )
    
    def reserve_space(self,
                      hostname: str,
                      host_address: str,
                      data_directory: str,
                      dbid: int,
                      required_size: SegmentSize) -> None:
        
        dirs_to_reserve = [(data_directory, required_size.datadir_size_kb)]
        tablespace_dirs = {os.path.dirname(dir) for dir in (required_size.tablespace_usage or {})}
        for tbl_dir in tablespace_dirs:
            # Sum all tablespace paths that live under this dir
            tbl_size_kb = sum(size_kb
                for tbl_path, size_kb in required_size.tablespace_usage.items()
                if os.path.dirname(tbl_path) == tbl_dir)
            dirs_to_reserve.append((tbl_dir, tbl_size_kb))
        for directory, size_kb in dirs_to_reserve:
            space_info = self._get_or_fetch_space_info(hostname, host_address, directory)
            self._reserve_space(
                fs_key=(hostname, space_info.filesystem),
                space_info=space_info,
                directory=directory,
                required_kb=size_kb,
                dbid=dbid,
                is_tablespace=(directory in tablespace_dirs)
            )
        
    def _format_space_issue(self, issue: Dict) -> str:
        """
        Format a space issue for error reporting
        """
        # Build directory breakdown
        dir_info = []
        if issue.get('num_datadirs', 0) > 0:
            dir_info.append(f"{issue['num_datadirs']} datadir(s)")
        if issue.get('num_tablespaces', 0) > 0:
            dir_info.append(f"{issue['num_tablespaces']} tablespace(s)")

        dir_breakdown = ', '.join(dir_info) if dir_info else 'unknown'

        return (
            f"\n  Host: {issue['hostname']}\n"
            f"    Filesystem: {issue['filesystem']}\n"
            f"    Directories: {dir_breakdown}\n"
            f"    Paths: {', '.join(issue['target_dirs'])}\n"
            f"    Segments affected: {issue['num_segments']}\n"
            f"    Required: {issue['required_gb']:.2f} GB\n"
            f"    Available: {issue['available_gb']:.2f} GB\n"
        )
    
    def get_allocated_space_for_filesystem(self, hostname: str, filesystem: str) -> int:
        """
        Get the space already allocated to a specific filesystem
            
        Returns:
            Allocated space in KB, or 0 if no allocations found
        """
        fs_key = (hostname, filesystem)
        if fs_key in self.filesystem_allocations:
            return self.filesystem_allocations[fs_key]['required_kb']
        return 0
    
    def check_space(self,
                    hostname: str,
                    host_address: str,
                    data_directory: str,
                    required_size: SegmentSize) -> Tuple[bool, int, str]:
        """
        Check if a data and tablespaces directories have enough space.
        
        Args:
            hostname: Target hostname
            host_address: Target host address
            data_directory: Target directory path
            requied_size: Required space in SegmentSize
            
        Returns:
            (has_space, min_available_kb, comma_separated_filesystems)            
        """
        dirs_to_check = [(data_directory, required_size.datadir_size_kb)]
        tablespace_dirs = {os.path.dirname(dir) for dir in (required_size.tablespace_usage or {})}
        for tbl_dir in tablespace_dirs:
            # Sum all tablespace paths that live under this dir
            tbl_size_kb = sum(size_kb
                for tbl_path, size_kb in required_size.tablespace_usage.items()
                if os.path.dirname(tbl_path) == tbl_dir)
            dirs_to_check.append((tbl_dir, tbl_size_kb))
        
        # Resolve each directory to a filesystem and accumulate per-filesystem
        # requirements.
        fs_requirements: Dict[str, int] = defaultdict(int)
        fs_space_info: Dict[str, DiskSpaceInfo] = {}

        for directory, size_kb in dirs_to_check:
            space_info = self._get_or_fetch_space_info(hostname, host_address, directory)
            if not space_info:
                return False, 0, 'unknown'
            fs = space_info.filesystem
            fs_requirements[fs] += int(size_kb * (1 + DISK_SPACE_SAFETY_MARGIN))
            fs_space_info[fs] = space_info

        fs_available: Dict[str, int] = {}
        for fs, required_kb in fs_requirements.items():
            fs_key = (hostname, fs)
            already_allocated_kb = self.filesystem_allocations[fs_key].required_kb \
                if fs_key in self.filesystem_allocations else 0
            available_kb = fs_space_info[fs].available_kb - already_allocated_kb
            fs_available[fs] = available_kb

            if available_kb < required_kb:
                self.logger.debug(
                    f"Host {hostname}: insufficient space on {fs}\n"
                    f"  Available: {available_kb / 1024 / 1024:.2f} GB\n"
                    f"  Required:  {required_kb / 1024 / 1024:.2f} GB"
                )
                return False, min(fs_available.values()), ', '.join(fs_requirements)

        return True, min(fs_available.values()), ', '.join(fs_requirements)

