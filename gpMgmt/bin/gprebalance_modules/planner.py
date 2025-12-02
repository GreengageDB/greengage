#!/usr/bin/env python3

from collections import defaultdict
import os
import pickle
import re
from typing import Any, Set, Tuple, List, Dict
from dataclasses import dataclass
from copy import deepcopy

from gppylib.db import dbconn
import gppylib.gparray as gparray
from gprebalance_modules.rebalance_commons import Host, HostStatus, CandidateSegment
from gprebalance_modules.solver import GreedySolver, HostId, SolverConfig

class ValidationError(Exception):
    pass
class PlanningError(Exception):
    pass

PRIMARY_PATH = '/data/primary/gpseg'
MIRROR_PATH = '/data/mirror/gpseg'

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
    seg: CandidateSegment
    dstHost: Host
    target_datadir: str
    target_port: int

    def __str__(self):
        """Pretty print logical move"""
        seg_info = self.seg.info
        
        # Source information
        src_host = self.seg.host.hostname
        src_datadir = seg_info.datadir
        src_port = seg_info.port
        
        # Destination information
        dst_host = self.dstHost.hostname
        dst_datadir = self.target_datadir
        dst_port = self.target_port
        
        # Size information (if available)
        size_str = ""
        if self.seg.size:
            size_str = str(self.seg.size)

        return (
            f"Move Segment(content={seg_info.content}, dbid={seg_info.dbid}, "
            f"role={seg_info.role}){size_str}\n"
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
                            strategy: str) -> Tuple[SolverConfig, HostMapping]:
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
                                                                       address=pair.primaryDB.address)]
            mirror_plcmnt[pair.mirrorDB.content] = host_mapping[Host(hostname=pair.mirrorDB.hostname,
                                                                     address=pair.mirrorDB.address)]
        n_initial = len(target_hosts)
        n_target = sum([1 for h in target_hosts if h.status != HostStatus.DECOMMISSIONED])
        conf = SolverConfig(gparray.get_primary_count(),
                            n_target,
                            n_initial,
                            primary_plcmnt,
                            mirror_plcmnt,
                            strategy)
        return (conf, host_mapping)

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
        if options.target_datadirs:
            self.dir_template_p, self.dir_template_m = self.get_datadirs()
        else:
            self.dir_template_p, self.dir_template_m = PRIMARY_PATH, MIRROR_PATH
        self.target_hosts, self.host_set_changed = Planner.get_target_hosts(self.virtual_gparray,
                                                                            options,
                                                                            self.dir_template_p,
                                                                            self.dir_template_m)
    
    def plan(self) -> Plan:
        # TODO Remove with future development. Temp code before state machine is implemented.
        from gprebalance_modules.rebalance_schema import RebalanceSchema
        conn = dbconn.connect(self.dburl, encoding='UTF8')
        rebalance_schema = RebalanceSchema(conn)
        if rebalance_schema.schemaExists():
            state_from_prev_run = rebalance_schema.getStateFromPreviousRun()
            if state_from_prev_run in ['STATE_SHRINK_TABLES_DONE',
                                       'STATE_SHRINK_CATALOG_STARTED',
                                       'STATE_SHRINK_CATALOG_DONE',
                                       'STATE_SHRINK_SEGMENTS_STOP_STARTED',
                                       'STATE_SHRINK_SEGMENTS_STOP_DONE',
                                       'STATE_SHRINK_DONE']:
                plan = ShrinkPlan([])
                plan.setTargetSegmentCount(self.options.target_segment_count)
                conn.close()
                return plan
        conn.close()
        
        # Planning starts here
        plan = Plan()

        self.validate_segment_status()
        if self.options.target_segment_count < self.gparray.get_segment_count():
            plan = self.plan_shrink()

        elif self.options.target_segment_count > self.gparray.get_segment_count():
            raise PlanningError("Expand is not supported yet")

        if self.options.skip_rebalance:
            self.logger.info("Skipping rebalance")
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
                         options: Any,
                         dir_template_p: str = PRIMARY_PATH,
                         dir_template_m: str = MIRROR_PATH) -> Tuple[List[Host], bool]:
        """
        Form set of hosts where we need to rebalance segments on
        """
        hosts = {}
        host_set_changed = False
        for seg in array.segmentPairs:
            if seg.primaryDB.content >= 0:
                if seg.primaryDB.hostname not in hosts:
                    hosts[seg.primaryDB.hostname] = Host(hostname=seg.primaryDB.hostname,\
                                               address=seg.primaryDB.address,\
                                               primary_datadirs={dir_template_p},\
                                               mirror_datadirs={dir_template_m},\
                                               status = HostStatus.ACTIVE)
                if seg.mirrorDB.hostname not in hosts:
                    hosts[seg.mirrorDB.hostname] = Host(hostname=seg.mirrorDB.hostname,\
                                               address=seg.mirrorDB.address,\
                                               primary_datadirs={dir_template_p},\
                                               mirror_datadirs={dir_template_m},\
                                               status = HostStatus.ACTIVE)
        for pair in array.segmentPairs:
            primary = pair.primaryDB
            mirror = pair.mirrorDB
            hosts[primary.hostname].primary_datadirs.add(
                re.sub(r'\d+$', '',primary.datadir))
            if mirror:
                hosts[mirror.hostname].mirror_datadirs.add(
                    re.sub(r'\d+$', '',mirror.datadir))
        if options.target_hosts:
            hl = list(map(str.strip, options.target_hosts.split(',')))
            for host in hosts.keys():
                if host not in hl:
                    hosts[host].status = HostStatus.DECOMMISSIONED
                    host_set_changed = True
            for host in hl:
                if host not in hosts:
                    hosts[host] = Host(hostname=host,\
                                       address=host,\
                                       primary_datadirs={dir_template_p},\
                                       mirror_datadirs={dir_template_m},\
                                       status = HostStatus.NEW)
                    host_set_changed = True
        if options.add_hosts:
            hl = list(map(str.strip, options.add_hosts.split(',')))
            for host in hl:
                if host not in hosts:
                    hosts[host] = Host(hostname=host,\
                                       address=host,\
                                       primary_datadirs={dir_template_p},\
                                       mirror_datadirs={dir_template_m},\
                                       status = HostStatus.NEW)
                    host_set_changed = True
        if options.remove_hosts:
            hl = list(map(str.strip, options.remove_hosts.split(',')))
            for host in hosts.keys():
                if host in hl:
                    hosts[host].status = HostStatus.DECOMMISSIONED
                    host_set_changed = True

        return hosts.values(), host_set_changed
    
    def get_datadirs(self) -> Tuple[str, str]:
        parts = self.options.target_datadirs.split(',')
        if len(parts) != 2:
            raise ValidationError('--target_datadirs options should have format like '
                                  '"/data/primary/gpseg{content} , /data/mirror/gpseg{content}"')
        # Remove {content} placeholder from each part
        cleaned_parts = []
        for part in parts:
            # Strip whitespace
            part = part.strip()
            match = re.search(r'\{.*\}$', part)
            if not match:
                raise ValidationError('--target_datadirs options should have format like '
                                  '"/data/primary/gpseg{content} , /data/mirror/gpseg{content}"')
            # Remove trailing {...}
            part = re.sub(r'\{.*\}$', '', part)
            cleaned_parts.append(part)

        return tuple(cleaned_parts)

    def form_moves(self) -> List[LogicalMove]:
        self.logger.info("Validation of rebalance possibility")

        if not self.virtual_gparray.hasMirrors:
            raise ValidationError("Cluster has mirroring disabled. Can't proceed with rebalance")

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
        config, host_mapping = ConfigurationEncoder.encode_configuration(self.virtual_gparray, self.target_hosts, strat)
        id_to_host = {v: k for k, v in host_mapping.items()}
        self.logger.info("Planning rebalance moves. Can take up to 60s.")
        planning_seed_value = int.from_bytes(os.urandom(16) , 'big')
        self.logger.info(f"Running randomized plan improvement with seed:{planning_seed_value}")
        solution, cost = GreedySolver(config, seed=planning_seed_value).solve()
        moves = []
        for pair in self.virtual_gparray.segmentPairs:
            prim = pair.primaryDB
            mir = pair.mirrorDB
            plcmnt = solution[prim.content]
            # TODO - resource estimation, ports, directories, size planning
            if host_mapping[Host(prim.hostname, prim.address)] != plcmnt[0]:
                cseg = CandidateSegment(prim, [h for h in self.target_hosts if h == Host(prim.hostname, prim.address)][0], None)
                moves.append(LogicalMove(cseg, id_to_host[plcmnt[0]], list(id_to_host[plcmnt[0]].primary_datadirs)[-1] + str(prim.content), 7002))
            if host_mapping[Host(mir.hostname, mir.address)] != plcmnt[1]:
                cseg = CandidateSegment(mir, [h for h in self.target_hosts if h == Host(mir.hostname, mir.address)][0], None)
                moves.append(LogicalMove(cseg, id_to_host[plcmnt[1]], list(id_to_host[plcmnt[1]].mirror_datadirs)[-1] + str(mir.content), 7003))
        
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
