#!/usr/bin/env python3

from collections import defaultdict
from contextlib import closing
import os
import pickle
from typing import Any, Tuple, List, Dict
from dataclasses import dataclass
from copy import deepcopy

from gppylib.db import dbconn
import gppylib.gparray as gparray
from gprebalance_modules.rebalance_commons import *
from gprebalance_modules.solver import GreedySolver, HostId, SolverConfig
from gppylib.commands.unix import PortIsAvailable

class PlanningError(Exception):
    pass

GROUPED = 'grouped'
SPREAD = 'spread'

HostMapping = Dict[Host, HostId]

@dataclass
class LogicalMove:
    """
    Move operation assumed by rebalance
    
    Attributes:
        seg: segment info
        dst: destination host
        target_datadir: target data directory
        target_port: target port
    """
    seg: Segment
    srcHost: Host
    dstHost: Host
    target_datadir: str
    target_port: int
    segment_size: Optional[SegmentSize] = None

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
            f"      From: {src_host}:{src_port} → {src_datadir}\n"
            f"      To:   {dst_host}:{dst_port} → {dst_datadir}"
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
        else:
            if not self.options.skip_resource_estimation:
                try:
                    with closing(dbconn.connect(self.dburl, encoding='UTF8')) as conn:
                        estimator = ResourceEstimator(
                            self.logger, 
                            conn, 
                            self.virtual_gparray,
                            batch_size=getattr(self.options, 'batch_size', 16)
                        )
                        estimator.estimate_and_validate_moves(rebalance_moves)
                except ResourceError as e:
                    raise PlanningError(f"Resource validation failed: {e}")
            else:
                self.logger.warning("Skipping resource estimation")

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
        planning_seed_value = int.from_bytes(os.urandom(16) , 'big')
        self.logger.info(f"Running randomized plan improvement with seed:{planning_seed_value}")
        solution, _ = GreedySolver(config, seed=planning_seed_value).solve()

        self.logger.debug(f"Solution: {solution}")
        self.logger.debug(f"Hosts: {id_to_host}")

        port_allocator = PortAllocator(self.virtual_gparray,
                                       self.logger,
                                       not self.options.skip_resource_estimation)
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

        return moves
    
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

class ResourceEstimator:
    """
    Estimates and validates resource requirements for segment moves
    
    This class handles move-specific resource estimation logic,
    using DiskSpaceChecker for invoking remote disk operations.
    """
    
    def __init__(self, logger: Any, conn: Any, gparray: gparray.GpArray, batch_size: int = 16):
        """
        Initialize resource estimator
        
        Args:
            logger: Logger instance
            conn: Database connection
            gparray: Current GpArray configuration
            batch_size: Number of parallel operations
        """
        self.logger = logger
        self.conn = conn
        self.gparray = gparray
        self.disk_checker = DiskSpaceChecker(logger, batch_size)
    
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
        
        try:
            # Step 1: Estimate segment sizes
            self._estimate_segment_sizes(moves)
            
            # Step 2: Validate available space on target hosts
            self._validate_target_space(moves)
            
            # Log summary
            total_size_kb = sum(move.segment_size.total_size_kb for move in moves if move.segment_size)
            total_size_gb = total_size_kb / 1024 / 1024
            self.logger.info(f"Total data to move: {total_size_gb:.2f} GB")
            self.logger.info("Resource validation completed successfully")
            
        except Exception as e:
            self.logger.error(f"Resource validation failed")
            raise ResourceError(str(e))
    
    def _estimate_segment_sizes(self, moves: List[LogicalMove]) -> None:
        """
        Estimate sizes for all segments being moved
        
        Populates the segment_size attribute on each LogicalMove
        """
        # Get unique segments to estimate
        segments_to_estimate = {move.seg.dbid: move.seg for move in moves}
        
        # Query for tablespace locations
        tablespace_map = self._get_tablespace_locations(list(segments_to_estimate.keys()))
        
        # Group segments by source host for batch disk usage
        segments_by_host = defaultdict(list)
        for move in moves:
            segments_by_host[move.srcHost.address].append(move.seg)
        
        # Estimate datadir sizes
        for host_addr, host_segments in segments_by_host.items():
            dirs = [seg.datadir for seg in host_segments]
            
            try:
                disk_usage = self.disk_checker.get_disk_usage(host_addr, dirs)
                
                for seg in host_segments:
                    size_kb = disk_usage.get(seg.datadir, 0)
                    # Find all moves for this segment and set size
                    for move in moves:
                        if move.seg.dbid == seg.dbid:
                            move.segment_size = SegmentSize(datadir_size_kb=size_kb)
                    
            except Exception as e:
                raise ResourceError(f"Cannot estimate segment sizes on host {host_addr}: {e}")
        
        # Estimate tablespace sizes
        self._estimate_tablespace_sizes(moves, tablespace_map)
    
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
            cursor = dbconn.query(self.conn, tablespace_location_sql)
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
            dbid = move.seg.dbid
            if dbid in tablespace_map:
                for tblspace_dir in tablespace_map[dbid]:
                    tblspace_by_host[move.srcHost.address].append((dbid, tblspace_dir))
        
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
    
    def _validate_target_space(self, moves: List[LogicalMove]) -> None:
        """
        Validate that target hosts have sufficient disk space

        Groups all space requirements by filesystem to handle cases where
        datadirs and tablespaces share the same underlying filesystem.
        """
        self.logger.info("Validating available disk space on target hosts...")

        # Collect all directories we need to check
        dirs_by_host = defaultdict(set)

        # Track what each directory is used for (for better error reporting)
        # (hostname, dir) -> moves in 'datadirs' if dir is datadir
        # moves moves in 'tablespaces' if dir is tablespace
        dir_usage = defaultdict(lambda: {'datadirs': [], 'tablespaces': []})

        for move in moves:
            if not move.segment_size:
                continue

            target_host = move.dstHost.hostname
            target_addr = move.dstHost.address
            target_datadir = move.target_datadir

            # Track datadir
            dirs_by_host[target_addr].add(target_datadir)
            dir_usage[(target_host, target_datadir)]['datadirs'].append(move)

            # Track tablespace directories
            if move.segment_size.tablespace_usage:
                for tblspace_path, _ in move.segment_size.tablespace_usage.items():
                    # Extract base path (remove /dbid suffix)
                    base_tblspace_path = os.path.dirname(tblspace_path)

                    dirs_by_host[target_addr].add(base_tblspace_path)
                    dir_usage[(target_host, base_tblspace_path)]['tablespaces'].append(move)

        # Convert sets to lists for DiskFree
        dirs_by_host = {host: list(dirs) for host, dirs in dirs_by_host.items()}

        # Check available space in batch
        try:
            space_info_by_host = self.disk_checker.check_batch_available_space(dirs_by_host)
        except Exception as e:
            raise ResourceError(f"Failed to check available disk space: {e}")

        # Group directories by filesystem and aggregate space requirements
        self.logger.info("Aggregating space requirements by filesystem...")
        issues = self._validate_filesystem_requirements(
            dir_usage=dir_usage,
            space_info_by_host=space_info_by_host
        )

        # Report all issues
        if issues:
            error_lines = ["Insufficient disk space for rebalance operation:\n"]

            for issue in issues:
                error_lines.append(self._format_space_issue(issue))

            error_lines.append(
                f"\nNote: Estimates include {int(DISK_SPACE_SAFETY_MARGIN * 100)}% safety margin"
            )
            raise ResourceError(''.join(error_lines))

        self.logger.info("Disk space validation completed successfully")

    def _validate_filesystem_requirements(self,
                                          dir_usage: Dict[Tuple[str, str], Dict],
                                          space_info_by_host: Dict[str, Dict[str, DiskSpaceInfo]]) -> List[Dict]:
        """
        Validate space requirements aggregated by filesystem

        This correctly handles cases where datadirs and tablespaces share the same filesystem.

        Args:
            dir_usage: Dict mapping (hostname, directory) -> {'datadirs': [moves], 'tablespaces': [moves]}
            space_info_by_host: Space information from DiskSpaceChecker

        Returns:
            List of issue dicts for filesystems with insufficient space
        """
        # Group by filesystem
        filesystem_requirements = defaultdict(lambda: {
            'required_kb': 0,
            'datadir_moves': set(),
            'tablespace_moves': set(),
            'datadirs': set(),
            'tablespaces': set(),
            'space_info': None
        })

        for (hostname, directory), usage in dir_usage.items():
            # Find the host address for this hostname
            host_address = None
            for moves_list in [usage['datadirs'], usage['tablespaces']]:
                if moves_list:
                    host_address = moves_list[0].dstHost.address
                    break
                
            if not host_address:
                continue

            if host_address not in space_info_by_host:
                raise ResourceError(f"No disk space information for host {hostname}")

            space_info = space_info_by_host[host_address].get(directory)
            if not space_info:
                raise ResourceError(
                    f"No disk space information for {hostname}:{directory}"
                )

            # Group by filesystem
            fs_key = (hostname, space_info.filesystem)
            fs_data = filesystem_requirements[fs_key]
            fs_data['space_info'] = space_info

            # Process datadir moves
            for move in usage['datadirs']:
                if not move.segment_size:
                    continue

                # Only count each move once per filesystem
                if move.seg.dbid not in fs_data['datadir_moves']:
                    fs_data['datadir_moves'].add(move.seg.dbid)
                    fs_data['datadirs'].add(directory)
                    size_kb = move.segment_size.datadir_size_kb
                    fs_data['required_kb'] += int(size_kb * (1 + DISK_SPACE_SAFETY_MARGIN))

            # Process tablespace moves
            for move in usage['tablespaces']:
                if not move.segment_size or not move.segment_size.tablespace_usage:
                    continue
                
                # Extract tablespace sizes that belong to this directory
                for tbl_path, size_kb in move.segment_size.tablespace_usage.items():
                    tbl_base_path = os.path.dirname(tbl_path)

                    if tbl_base_path == directory:
                        # Use a unique key to avoid double-counting the same tablespace
                        tbl_key = (move.seg.dbid, tbl_path)

                        if tbl_key not in fs_data['tablespace_moves']:
                            fs_data['tablespace_moves'].add(tbl_key)
                            fs_data['tablespaces'].add(directory)
                            fs_data['required_kb'] += int(size_kb * (1 + DISK_SPACE_SAFETY_MARGIN))

        # Validate each filesystem
        issues = []

        for (hostname, filesystem), fs_data in filesystem_requirements.items():
            required_kb = fs_data['required_kb']
            available_kb = fs_data['space_info'].available_kb

            required_gb = required_kb / 1024 / 1024
            available_gb = available_kb / 1024 / 1024

            # Count unique segments
            unique_segments = len(fs_data['datadir_moves']) + len({dbid for dbid, _ in fs_data['tablespace_moves']})

            self.logger.debug(
                f"Filesystem {filesystem} on {hostname}: "
                f"Required {required_gb:.2f} GB, Available {available_gb:.2f} GB "
                f"({unique_segments} segments, "
                f"{len(fs_data['datadirs'])} datadirs, "
                f"{len(fs_data['tablespaces'])} tablespace dirs)"
            )

            if available_kb < required_kb:
                all_dirs = sorted(fs_data['datadirs'].union(fs_data['tablespaces']))

                issues.append({
                    'type': 'filesystem',
                    'hostname': hostname,
                    'filesystem': filesystem,
                    'target_dirs': all_dirs,
                    'num_datadirs': len(fs_data['datadirs']),
                    'num_tablespaces': len(fs_data['tablespaces']),
                    'num_segments': unique_segments,
                    'required_gb': required_gb,
                    'available_gb': available_gb,
                })

        return issues

    def _format_space_issue(self, issue: Dict) -> str:
        """
        Format a space issue for error reporting
        """
        issue_type = issue.get('type', 'unknown').upper()

        # Build directory breakdown
        dir_info = []
        if issue.get('num_datadirs', 0) > 0:
            dir_info.append(f"{issue['num_datadirs']} datadir(s)")
        if issue.get('num_tablespaces', 0) > 0:
            dir_info.append(f"{issue['num_tablespaces']} tablespace(s)")

        dir_breakdown = ', '.join(dir_info) if dir_info else 'unknown'

        return (
            f"\n  [{issue_type}] Host: {issue['hostname']}\n"
            f"    Filesystem: {issue['filesystem']}\n"
            f"    Directories: {dir_breakdown}\n"
            f"    Paths: {', '.join(issue['target_dirs'])}\n"
            f"    Segments affected: {issue['num_segments']}\n"
            f"    Required: {issue['required_gb']:.2f} GB\n"
            f"    Available: {issue['available_gb']:.2f} GB\n"
        )
