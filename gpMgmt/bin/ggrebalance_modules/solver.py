from dataclasses import dataclass
from enum import Enum
import math
import random
from typing import NewType, Optional, Set, Dict, List, Tuple, Union
from collections import defaultdict
import time

# 0 <= hostid <= n_hosts_initial
HostId = NewType('HostId', int)
# 0 <= contentid <= n_segments
ContentId = NewType('ContentId', int)
# number of moves
Cost = NewType('Cost', int)
# load
Load = NewType('Load', int)
# {'contendid' - > (primary host, mirror host)}
Solution = Dict[ContentId, Tuple[HostId, HostId]]

@dataclass
class SolverConfig:
    """
    Configuration for the balancing problem
    Attributes:
        n_segments: planned numsegments after shrink/expand
        n_hosts_target: target number of hosts
        n_hosts_initial: initial number of hosts
        initial_primary_mapping: initial_primary_mapping[contentid] = hostid
        initial_mirror_mapping: initial_mirror_mapping[contentid] = hostid, 0 <= hostid <= n_hosts_initial
        strategy: mirroring strategy
    """
    n_segments: int
    n_hosts_target: int
    n_hosts_initial: int
    initial_primary_mapping: List[HostId]
    initial_mirror_mapping: List[HostId]
    strategy: str

class GreedySolver:
    """
    Greedy construction of a balanced primary/mirror assignment
    with an optional ALNS improvement phase.

    Segments:
        0 .. n_segments-1

    Hosts:
        initial hosts: 0 .. n_hosts_initial-1
        target hosts:  0 .. n_hosts_target-1 (subset of initial hosts)

    Constraints:
        - Each segment has exactly one primary host and one mirror host.
        - Primary host != Mirror host for every segment.
        - Each target host has:
              target_primary_load primaries
              target_primary_load mirrors
          => total 2 * target_primary_load segments (primary+mirror) per host.
        - `strategy`:
            'grouped': all segments with same primary host share a single mirror host
            'spread' : all segments with same primary host have distinct mirror hosts
    """
    def __init__(self, 
                 config: SolverConfig,
                 run_improve: bool = True,
                 printing: bool = False,
                 seed: int = None):
        
        self.n_segments = config.n_segments
        self.n_hosts_target = config.n_hosts_target
        self.n_hosts_initial = config.n_hosts_initial
        self.initial_primary_mapping = config.initial_primary_mapping
        self.initial_mirror_mapping = config.initial_mirror_mapping
        self.strategy = config.strategy

        self.run_improve = run_improve
        self.printing = printing
        self.target_primary_load = self.n_segments // self.n_hosts_target
        self.target_load = 2 * self.n_segments // self.n_hosts_target

        if self.n_hosts_target < 2:
            raise ValueError("Cannot balance to single host")
        
        if self.n_segments % self.n_hosts_target != 0:
            raise ValueError(f"Cannot evenly distribute {self.n_segments}"
                             f"segments across {self.n_hosts_target} hosts")

        if self.strategy == 'spread':
            if self.target_primary_load > self.n_hosts_target - 1:
                raise ValueError("Cannot follow spread mirroring strategy")
        
        # Best known solution
        self.best_primary_mapping: List[HostId] | None = None
        self.best_mirror_mapping: List[HostId] | None = None
        self.best_cost = None

        # seed to use in ALNS
        self.seed = seed

    def solve(self) -> Tuple[Solution, Cost]:
        """
        Build an initial greedy solution and optionally improve it with ALNS.

        Returns:
            (solution, cost) where solution is a mapping from ContentId to
            (primary_host, mirror_host), and cost is number of moves from
            the initial placement.
        """
        # Phase 1: Balance primaries
        primary_mapping = self._balance_primaries()

        # Phase 2: Assign mirrors
        mirror_mapping = self._assign_mirror_hosts(primary_mapping)

        # Optional improvement by ALNS.
        if self.run_improve:
            # at least 10 iterations per segment
            config = ALNSConfig(max_iterations=10 * self.n_segments)
            alns = ALNS(self, config)
            if self.seed:
                random.seed(self.seed)
            primary_mapping, mirror_mapping = alns.optimize(primary_mapping, mirror_mapping)
                   
        solution: Solution = {
            ContentId(i): (HostId(primary_mapping[i]), HostId(mirror_mapping[i]))
            for i in range(self.n_segments)
        }
        
        cost = self._calculate_cost(primary_mapping, mirror_mapping)

        assert(self._validate_solution(solution))

        self.best_primary_mapping = primary_mapping
        self.best_mirror_mapping  = mirror_mapping

        return solution, cost

    # --------------------------------------------------------------------- #
    #  Phase 1: Primaries
    # --------------------------------------------------------------------- #
    def _balance_primaries(self) -> List[HostId]:
        """
        Assign primaries to target hosts, keeping:
            - all host loads equal to target_primary_load,
            - as many primaries as possible on their original host,
            - moving out primaries from:
                * hosts not in target count,
                * overloaded hosts.
        """
        primary_mapping = [-1] * self.n_segments
        
        # Count initial load only on target hosts.
        initial_load = [0] * self.n_hosts_initial
        for p in self.initial_primary_mapping:
            if not self.is_decomissioned(p):
                initial_load[p] += 1
        
        # Segment processing order:
        # must_move first (out-of-target or overloaded),
        # then by descending load on their original host (move from heavier first).
        segment_order = []
        for i in range(self.n_segments):
            orig_host = self.initial_primary_mapping[i]
            must_move = (
                self.is_decomissioned(orig_host)
                or initial_load[orig_host] > self.target_primary_load
            )
            segment_order.append((must_move, initial_load[orig_host], i))
        
        segment_order.sort(reverse=True, key=lambda x: (x[0], x[1]))

        # Deficit
        # Calculate how many segments each host needs to receive
        deficit = []
        for h in range(self.n_hosts_target):
            if h < self.n_hosts_initial:
                deficit.append(self.target_primary_load - initial_load[h])
            else:
                deficit.append(self.target_primary_load)
        
        # Current primary load for each target host.
        current_load = [0] * self.n_hosts_target
        
        for _, _, seg_id in segment_order:
            orig_host = self.initial_primary_mapping[seg_id]
            
            # Try to keep on original host if possible
            if (not self.is_decomissioned(orig_host) and 
                current_load[orig_host] < self.target_primary_load):
                host = orig_host
            else:
                # Find host with largest remaining deficit
                host = max(range(self.n_hosts_target), key=lambda h: deficit[h])

            primary_mapping[seg_id] = host
            current_load[host] += 1
            deficit[host] -= 1
        
        return primary_mapping
    
    def is_decomissioned(self, host: HostId):
        return host >= self.n_hosts_target

    # --------------------------------------------------------------------- #
    #  Phase 2: Mirrors
    # --------------------------------------------------------------------- #
    def _assign_mirror_hosts(self, primary_mapping: List[HostId]) -> List[HostId]:
        """
        Assign mirror hosts for all segments, respecting:
            - load balance
            - primary host != mirror host
            - chosen mirroring strategy (grouped/spread)
            - minimizing deviation from initial_mirror_mapping where possible.
        """
        mirror_mapping = [-1] * self.n_segments
        mirror_load = [0] * self.n_hosts_target

        # Group segments by primary host
        groups: Dict[HostId, List[ContentId]] = defaultdict(list)
        for seg in range(self.n_segments):
            groups[primary_mapping[seg]].append(seg)

        if self.strategy == 'grouped':
            self._assign_mirrors_grouped(primary_mapping, mirror_mapping, mirror_load, groups)
        elif self.strategy == 'spread':
            self._assign_mirrors_spread(primary_mapping, mirror_mapping, mirror_load)

        return mirror_mapping

    def _assign_mirrors_grouped(self,
                                primary_mapping: List[HostId],
                                mirror_mapping: List[HostId],
                                mirror_load: List[Load],
                                groups: Dict[HostId, List[int]]):
        """
        Grouped strategy:

        All segments that share a primary host p_host must share the same
        mirror host m_host (per-group mirroring).
        """
        # Track mirror choice for each primary host.
        phost_to_mhost: Dict[HostId, HostId] = {}

        preferences = self._compute_mirror_preferences(groups)

        sorted_p_hosts = sorted(groups.keys(),
                                key=lambda p: -max(preferences[p].values(), default=0))

        for p_host in sorted_p_hosts:
            segments = groups[p_host]
            best_mirror_host = self._select_group_mirror(
                p_host=p_host,
                mirror_mapping=mirror_mapping,
                mirror_load=mirror_load,
                phost_to_mhost=phost_to_mhost,
                groups=groups,
                preferences=preferences
            )
            # Fallback to straightforward assignment (shouldn't happen)
            if best_mirror_host is None:
                self._fill_naive_grouped(primary_mapping, mirror_mapping)
                return
            # Assign the group to chosen mirror.
            for seg in segments:
                mirror_mapping[seg] = best_mirror_host
                mirror_load[best_mirror_host] += 1

            phost_to_mhost[p_host] = best_mirror_host
    
    def _fill_naive_grouped(self,
                            primary_mapping: List[HostId],
                            mirror_mapping: List[HostId]):
        """
        GROUPED: Mirrors from same primary host lie at the same host
        """
        for content, primary_host in enumerate(primary_mapping):
            mirror_mapping[content] = (primary_host + 1) % self.n_hosts_target
        
    def _fill_naive_spread(self,
                       primary_mapping: List[HostId],
                       mirror_mapping: List[HostId]):
        """
        SPREAD: Mirrors from same primary host spread across different hosts
        """
        segments_per_primary = defaultdict(int)

        for content_id, primary_host in enumerate(primary_mapping):
            # Calculate local index within primary host's segments
            local_idx = segments_per_primary[primary_host]
            segments_per_primary[primary_host] += 1

            # Formula: mirror = (primary + local_idx + 1) % n_hosts
            mirror_host = (primary_host + 1 + local_idx) % self.n_hosts_target

            # Ensure no colocation
            if mirror_host == primary_host:
                mirror_host = (mirror_host + 1) % self.n_hosts_target

            mirror_mapping[content_id] = mirror_host

    def _compute_mirror_preferences(self,
                                    groups: Dict[HostId, List[ContentId]]) -> Dict[HostId, Dict[HostId, int]]:
        """
        Precompute uses (preferences) for each primary host -> possible mirror host.
        Returns: {p_host: {m_host: use_count}}
        """
        preferences: Dict[HostId, Dict[HostId, int]] = defaultdict(lambda: defaultdict(int))
        for p_host, segments in groups.items():
            for seg in segments:
                orig_mirror_host = self.initial_mirror_mapping[seg]
                if not self.is_decomissioned(orig_mirror_host) and orig_mirror_host != p_host:
                    preferences[p_host][orig_mirror_host] += 1
        return preferences

    def _select_group_mirror(self,
                             p_host: HostId,
                             mirror_mapping: List[HostId],
                             mirror_load: List[Load],
                             phost_to_mhost: Dict[HostId, HostId],
                             groups: Dict[HostId, List[ContentId]],
                             preferences: Dict[HostId, Dict[HostId, int]]) -> HostId:
        """
        Pick a mirror host for a primary group (grouped strategy),
        with the following priority:
            1. Most-used original mirror host.
            2. Least loaded available host with least contention by other primaries.
            3. Swap another already-assigned group with current in
            case of conflict when the only mirror host is primary one.
        """    

        mirror_uses = preferences[p_host]
        segs = groups[p_host]
        group_size = len(segs)
        assigned_p_hosts = set(phost_to_mhost.keys())

        best_mirror_host: HostId | None = None

        # Priority 1: Most used original mirror host (if has capacity)
        if mirror_uses:
            candidates_with_capacity = []
            for h, uses in mirror_uses.items():
                if mirror_load[h] + group_size <= self.target_primary_load:
                    # Contention: sum of uses from unassigned groups for this host
                    contention = sum(preferences[other_p].get(h, 0) 
                               for other_p in preferences 
                               if other_p != p_host and other_p not in assigned_p_hosts)
                    candidates_with_capacity.append((uses, contention, mirror_load[h], h))
            if candidates_with_capacity:
                candidates_with_capacity.sort(key=lambda x: (-x[0], x[1], x[2], x[3]))
                best_mirror_host = candidates_with_capacity[0][3]

        # Priority 2: Least loaded host
        if best_mirror_host is None:
            available_hosts = []
            for h in range(self.n_hosts_target):
                if h != p_host and mirror_load[h] + group_size <= self.target_primary_load:
                    contention = sum(preferences[other_p].get(h, 0) 
                                    for other_p in preferences 
                                    if other_p != p_host and other_p not in assigned_p_hosts)
                    available_hosts.append((contention, mirror_load[h], h))
            if available_hosts:
                best_mirror_host = min(available_hosts)[2]

        # Priority 3: DEADLOCK - Try swapping with already assigned group
        if best_mirror_host is None:
            best_mirror_host = self._swap_to_resolve_deadlock(
                blocked_p_host=p_host,
                mirror_load=mirror_load,
                phost_to_mhost=phost_to_mhost,
                groups=groups,
                mirror_mapping=mirror_mapping)
        
        return best_mirror_host

    def _swap_to_resolve_deadlock(self, 
                                blocked_p_host: HostId,
                                mirror_load: List[Load],
                                phost_to_mhost: Dict[HostId, HostId],
                                groups: Dict[HostId, List[ContentId]],
                                mirror_mapping: List[HostId]):
        """
        Resolve a deadlock in grouped mirroring by moving another
        mirror group to a different mirror.

        Idea:
            - Look at already assigned primary hosts (other_p_host).
            - For each, see if we can move that group to some alternative mirror.
            - If that frees enough capacity on its current mirror for blocked_p_host,
              we do the move and return the freed mirror as the new mirror for
              blocked_p_host.
        """

        for other_p_host, current_mirror_host in phost_to_mhost.items():
            if other_p_host == blocked_p_host:
                continue
            
            other_size = len(groups[other_p_host])

            # Find alternative mirror where to move other_p_host's mirror group
            alternative_mirror_host = next(
                (h for h in range(self.n_hosts_target)
                 if h != other_p_host and  # Can't be other's primary
                    mirror_load[h] + other_size <= self.target_primary_load),
                None
            )

            if alternative_mirror_host is None:
                continue 
            
            space_after_move = mirror_load[current_mirror_host] - other_size
            if space_after_move + len(groups[blocked_p_host]) > self.target_primary_load:
                continue
            
            # Swap: move other_p_host to alternative_mirror
            for seg in groups[other_p_host]:
                mirror_mapping[seg] = alternative_mirror_host

            mirror_load[current_mirror_host] -= other_size
            mirror_load[alternative_mirror_host] += other_size
            phost_to_mhost[other_p_host] = alternative_mirror_host

            return current_mirror_host

        return None

    def _assign_mirrors_spread(self,
                               primary_mapping: List[HostId],
                               mirror_mapping: List[HostId],
                               mirror_load: List[Load]):
        """
        Spread strategy:

        For each primary host p_host, the mirrors of all segments in that group
        must be distinct and not equal to p_host.
        """

        used_in_group: Dict[HostId, Set[HostId]] = defaultdict(set)
        
        # Phase 1: Try to assign segments to their original mirrors
        unassigned: List[ContentId] = []

        for seg in range(self.n_segments):
            p_host = primary_mapping[seg]
            orig_mirror_host = self.initial_mirror_mapping[seg]

            # Check if original mirror is valid and available
            can_use_original = (
                not self.is_decomissioned(orig_mirror_host) and
                orig_mirror_host != p_host and
                orig_mirror_host not in used_in_group[p_host] and
                mirror_load[orig_mirror_host] < self.target_primary_load
            )

            if can_use_original:
                mirror_mapping[seg] = orig_mirror_host
                mirror_load[orig_mirror_host] += 1
                used_in_group[p_host].add(orig_mirror_host)
            else:
                unassigned.append(seg)

        # Phase 2: Assign remaining segments with load balancing
        for seg in unassigned:
            p_host = primary_mapping[seg]

            available = [
                    h for h in range(self.n_hosts_target)
                    if (h != p_host and
                        h not in used_in_group[p_host] and
                        mirror_load[h] < self.target_primary_load)
                    ]
            
            if available:
                best_host =  min(available, key=lambda h: mirror_load[h])
                mirror_mapping[seg] = best_host
                mirror_load[best_host] += 1
                used_in_group[p_host].add(best_host)
            else:
                # Priority 3: DEADLOCK - try swap
                best_host = self._resolve_spread_deadlock_for_segment(seg,
                                                                      primary_mapping,
                                                                      mirror_mapping,
                                                                      mirror_load,
                                                                      used_in_group)
                
                if best_host is None:
                    self._fill_naive_spread(primary_mapping, mirror_mapping)
                    return

                mirror_mapping[seg] = best_host
                mirror_load[best_host] += 1
                used_in_group[p_host].add(best_host)

    def _resolve_spread_deadlock_for_segment(self,
                                             seg: ContentId,
                                             primary_mapping: List[HostId],
                                             mirror_mapping: List[HostId],
                                             mirror_load: List[Load],
                                             used_in_group: Dict[HostId, Set[HostId]],
                                             ) -> Optional[HostId]:
        """
        Attempt to resolve a deadlock for one segment in the spread strategy
        by relocating another segment's mirror to a host with capacity.
        """
        p_host = primary_mapping[seg]

        hosts_with_capacity = [
                h for h in range(self.n_hosts_target)
                if mirror_load[h] < self.target_primary_load]
        
        if not hosts_with_capacity:
            return None
        
        # Find hosts we could use (not in our group, at any load level)
        candidate_hosts = [
            h for h in range(self.n_hosts_target)
            if h != p_host and h not in used_in_group[p_host]
        ]
        # Try to find a segment using one of these hosts that can move mirror to p_host
        for candidate_host in candidate_hosts:
            # Find segments currently using candidate_host as mirror
            for other_seg in range(self.n_segments):
                if mirror_mapping[other_seg] != candidate_host:
                    continue
                
                other_p_host = primary_mapping[other_seg]
                # Check if other_seg can use p_host as mirror
                for dest_host in hosts_with_capacity:
                    # Check if other_seg can move to dest_host
                    if dest_host == other_p_host:
                        continue
                    if dest_host in used_in_group[other_p_host]:
                        continue
                    
                    # Perform the swap
                    # Remove other_seg from candidate_host
                    used_in_group[other_p_host].remove(candidate_host)
                    mirror_load[candidate_host] -= 1

                    # Move other_seg to dest_host
                    mirror_mapping[other_seg] = dest_host
                    used_in_group[other_p_host].add(dest_host)
                    mirror_load[dest_host] += 1
                    
                    return candidate_host

        return None

    def _calculate_cost(self, primary: List[HostId], mirror: List[HostId]) -> int:
        """
        Calculate movement cost
        """
        return sum(
            (1 if primary[i] != self.initial_primary_mapping[i] else 0) +
            (1 if mirror[i] != self.initial_mirror_mapping[i] else 0)
            for i in range(self.n_segments)
        )

    def _validate_solution(self, solution: Solution) -> bool:
        """
        Validate that solution satisfies all constraints
        """
        
        # Check 1: All segments assigned
        if len(solution) != self.n_segments:
            return False
        
        # Check 2: Primary host != Mirror host 
        for i, (p, m) in solution.items():
            if p == m:
                return False
        
        # Check 3: Load balance
        load = [0] * (self.n_hosts_target)
        for i, (p, m) in solution.items():
            load[p] += 1
            load[m] += 1
        
        for h in range(self.n_hosts_target):
            if load[h] != self.target_primary_load * 2 :
                return False
        
        # Check 4: Strategy constraints
        segments_by_host = defaultdict(list)
        for i, (p, m) in solution.items():
            segments_by_host[p].append((i, m))
        
        for _, segs in segments_by_host.items():
            if len(segs) < 2:
                continue
            
            mirror_hsts = [r for (i, r) in segs]
            
            if self.strategy == 'grouped':
                if len(set(mirror_hsts)) > 1:
                    return False
            elif self.strategy == 'spread':
                if len(set(mirror_hsts)) != len(mirror_hsts):
                    return False
        
        return True


@dataclass
class ALNSConfig:
    """
    ALNS algorithm configuration.
    """
    max_iterations: int = 1000
    timeout: float = 60.0
    temperature_decay: float = 0.95
    min_temperature: float = 0.01
    local_search_frequency: int = 5
    stagnation_threshold: int = 30
    restart_threshold: int = 20


@dataclass
class SearchState:
    """
    Current search state and statistics.
    """
    current_primary_mapping: List[HostId]
    current_mirror_mapping: List[HostId]
    current_cost: Cost
    best_primary_mapping: List[HostId]
    best_mirror_mapping: List[HostId]
    best_cost: Cost
    iteration: int = 0
    stagnation_count: int = 0
    last_improvement_iter: int = 0

    def get_temperature(self, config: ALNSConfig) -> float:
        """
        Calculate current temperature.
        """
        temp = 1.0 * (config.temperature_decay ** self.iteration)
        return max(config.min_temperature, temp)

    def get_destroy_size(self, config: ALNSConfig) -> float:
        """
        Calculate adaptive destroy size based on progress.
        """
        if self.iteration - self.last_improvement_iter > config.restart_threshold:
            return random.uniform(0.20, 0.40)  # Stuck: larger neighborhoods
        elif self.iteration < config.max_iterations * 0.3:
            return random.uniform(0.15, 0.30)  # Early phase: explore
        else:
            return random.uniform(0.10, 0.20)  # Late phase: intensify

    def update_best(self, primary_mapping: List[HostId], mirror_mapping: List[HostId], cost: Cost) -> bool:
        """
        Update best solution found.
        """
        if cost < self.best_cost:
            self.best_primary_mapping = primary_mapping[:]
            self.best_mirror_mapping = mirror_mapping[:]
            self.best_cost = cost
            self.last_improvement_iter = self.iteration
            return True
        return False

    def should_restart(self, config: ALNSConfig) -> bool:
        """
        Check if search should restart from best.
        """
        return self.stagnation_count > config.stagnation_threshold

    def restart_from_best(self):
        """
        Restart search from best solution.
        """
        self.current_primary_mapping = self.best_primary_mapping[:]
        self.current_mirror_mapping = self.best_mirror_mapping[:]
        self.current_cost = self.best_cost
        self.stagnation_count = 0


class ALNSDestroyMethod(Enum):
    GROUP_DESTROY = 'group_destroy'
    BAD_SEGMENTS = 'bad_segments'
    SHAW_REMOVAL = 'shaw_removal'
    RANDOM_SEGMENTS = 'random_segments'


class ALNSDestroyOperators:
    """
    Collection of destroy operators for ALNS.
    """
    def __init__(self, n_segments: int, strategy: str, 
                 initial_primary: List[HostId],
                 initial_mirror: List[HostId]):
        self.n_segments = n_segments
        self.strategy = strategy
        self.initial_primary = initial_primary
        self.initial_mirror = initial_mirror

    def select_method(self, stagnation: int) -> ALNSDestroyMethod:
        """
        Select destroy method based on search state.
        """
        if stagnation > 15:
            # Stuck: use aggressive methods
            return random.choice([ALNSDestroyMethod.GROUP_DESTROY, ALNSDestroyMethod.RANDOM_SEGMENTS])
        
        if self.strategy == 'grouped':
            # For grouped, favor group-based destroy
            return random.choices(
                [ALNSDestroyMethod.GROUP_DESTROY,
                 ALNSDestroyMethod.BAD_SEGMENTS,
                 ALNSDestroyMethod.SHAW_REMOVAL,
                 ALNSDestroyMethod.RANDOM_SEGMENTS],
                weights=[0.4, 0.3, 0.2, 0.1]
            )[0]
        else:
            # For spread, favor segment-based methods
            return random.choices(
                [ALNSDestroyMethod.BAD_SEGMENTS,
                 ALNSDestroyMethod.SHAW_REMOVAL,
                 ALNSDestroyMethod.RANDOM_SEGMENTS],
                weights=[0.4, 0.4, 0.2]
            )[0]

    def destroy_random(self, destroy_size: float) -> Set[ContentId]:
        """
        Destroy random segments.
        """
        n_destroy = max(1, int(self.n_segments * destroy_size))
        return set(random.sample(range(self.n_segments), n_destroy))

    def destroy_primary_groups(self, primary: List[HostId], mirror: List[HostId],
                              destroy_size: float) -> Set[ContentId]:
        """
        Destroy complete primary groups (for grouped strategy).
        """
        groups = defaultdict(list)
        for seg in range(self.n_segments):
            groups[primary[seg]].append(seg)
        
        # Score groups by badness
        primary_badness = self._calculate_group_badness(groups, primary, mirror)
        
        # Select groups probabilistically
        return self._select_groups_by_badness(groups, primary_badness, destroy_size)

    def destroy_bad_segments(self, primary: List[HostId], mirror: List[HostId],
                            destroy_size: float) -> Set[ContentId]:
        """
        Destroy segments that differ from initial placement.
        """
        n_destroy = max(1, int(self.n_segments * destroy_size))
        
        # Find segments that moved from original position
        bad_segments = []
        for seg in range(self.n_segments):
            badness = 0
            if primary[seg] != self.initial_primary[seg]:
                badness += 1
            if mirror[seg] != self.initial_mirror[seg]:
                badness += 1
            
            if badness > 0:
                bad_segments.append((badness, seg))
        
        if not bad_segments:
            return self.destroy_random(destroy_size)
        
        return self._select_bad_segments_with_relatedness(bad_segments, primary, mirror, n_destroy)

    def shaw_removal(self, primary: List[HostId], mirror: List[HostId],
                    destroy_size: float) -> Set[ContentId]:
        """
        Remove related segments (same primary or mirror host).
        """
        n_destroy = max(1, int(self.n_segments * destroy_size))
        
        seed_seg = random.randint(0, self.n_segments - 1)
        destroyed = {seed_seg}
        
        # Calculate relatedness scores
        relatedness = []
        for seg in range(self.n_segments):
            if seg == seed_seg:
                continue
            
            score = 0
            if primary[seg] == primary[seed_seg]:
                score += 2
            if mirror[seg] == mirror[seed_seg]:
                score += 1
            
            relatedness.append((score, seg))
        
        # Sort by relatedness and take most related
        relatedness.sort(reverse=True)
        for _, seg in relatedness[:n_destroy-1]:
            destroyed.add(seg)
        
        return destroyed

    def _calculate_group_badness(self, groups: Dict[HostId, List[ContentId]], 
                                primary: List[HostId], mirror: List[HostId]) -> Dict[HostId, float]:
        """
        Calculate badness score for each primary group.
        (how many segments deviate from original)
        """
        badness = {}
        for p_host, segments in groups.items():
            moved_mirrors = sum(1 for seg in segments 
                              if mirror[seg] != self.initial_mirror[seg])
            moved_primaries = sum(1 for seg in segments 
                                if primary[seg] != self.initial_primary[seg])
            
            total_moved = moved_mirrors + moved_primaries
            badness[p_host] = total_moved / len(segments) if segments else 0
        
        return badness

    def _select_groups_by_badness(self, groups: Dict[HostId, List[ContentId]], 
                                 badness: Dict[HostId, float], 
                                 destroy_size: float) -> Set[ContentId]:
        """
        Select primary groups weighted by badness.
        """
        n_destroy = max(1, int(self.n_segments * destroy_size))
        primaries = list(groups.keys())
        weights = [badness.get(p, 0.0) + 0.1 for p in primaries]  # Add exploration constant 0.1
        
        destroyed = set()
        available_primaries = primaries[:]
        available_weights = weights[:]
        
        attempts = 0
        while len(destroyed) < n_destroy and attempts < 20 and available_primaries:
            p_host = random.choices(available_primaries, weights=available_weights)[0]
            destroyed.update(groups[p_host])
            
            # Remove from further selection
            idx = available_primaries.index(p_host)
            available_primaries.pop(idx)
            available_weights.pop(idx)
            attempts += 1
        
        # Trim to size if necessary
        if len(destroyed) > n_destroy:
            destroyed = set(random.sample(list(destroyed), n_destroy))
        
        return destroyed

    def _select_bad_segments_with_relatedness(self, bad_segments: List[Tuple[int, ContentId]], 
                                            primary: List[HostId], mirror: List[HostId],
                                            n_destroy: int) -> Set[ContentId]:
        """
        Select bad segments and add related ones.
        """
        bad_segments.sort(reverse=True)
        destroyed = set()
        
        # Start with worst segments
        for _, seg in bad_segments[:min(n_destroy, len(bad_segments))]:
            destroyed.add(seg)
        
        # Add related segments to reach quota
        if len(destroyed) < n_destroy:
            seed_segments = list(destroyed)
            for seed_seg in seed_segments:
                if len(destroyed) >= n_destroy:
                    break
                
                for seg in range(self.n_segments):
                    if seg in destroyed or len(destroyed) >= n_destroy:
                        continue
                    
                    if (primary[seg] == primary[seed_seg] or 
                        mirror[seg] == mirror[seed_seg]):
                        destroyed.add(seg)
        
        return destroyed


class ALNSRepairMethod(Enum):
    GREEDY_REPAIR = 'repair_greedy'
    CONSTRAINT_REPAIR = 'repair_constrained'
    REGRET_REPAIR = "repair_regret"


class ALNSRepairOperators:
    """Repair operators for ALNS."""

    def __init__(self, n_segments: int,
                 n_hosts: int, strategy: str,
                 initial_primary_mapping: List[HostId],
                 initial_mirror_mapping: List[HostId],
                 target_load: Load):
        self.n_segments = n_segments
        self.n_hosts = n_hosts
        self.strategy = strategy
        self.initial_primary_mapping = initial_primary_mapping
        self.initial_mirror_mapping = initial_mirror_mapping
        self.target_load = target_load
        self._host_set = set(range(self.n_hosts))

    def select_method(self, iteration: int, max_iterations: int) -> ALNSRepairMethod:
        """
        Select destroy method based on search state.
        """
        # Phase 1: Early exploration (first 20% of iterations)
        if iteration < max_iterations * 0.2:
            return random.choices(
                [ALNSRepairMethod.GREEDY_REPAIR,
                 ALNSRepairMethod.CONSTRAINT_REPAIR],
                weights=[0.9, 0.1]
            )[0]

        # Phase 2: Quality optimization (middle 60%)
        elif iteration < max_iterations * 0.8:
            if self.strategy == 'spread':
                # Spread is harder - more Most Constrained
                return random.choices(
                    [ALNSRepairMethod.CONSTRAINT_REPAIR,
                     ALNSRepairMethod.GREEDY_REPAIR,
                     ALNSRepairMethod.REGRET_REPAIR],
                    weights=[0.3, 0.5, 0.2]
                )[0]
            else:
                # Grouped is easier - more Greedy/Regret
                return random.choices(
                    [ALNSRepairMethod.GREEDY_REPAIR,
                     ALNSRepairMethod.REGRET_REPAIR,
                     ALNSRepairMethod.CONSTRAINT_REPAIR],
                    weights=[0.4, 0.4, 0.2]
                )[0]
    
        # Phase 3: Final intensification (last 20%)
        else:
            return random.choices(
                [ALNSRepairMethod.REGRET_REPAIR,
                 ALNSRepairMethod.GREEDY_REPAIR,
                 ALNSRepairMethod.CONSTRAINT_REPAIR],
                weights=[0.5, 0.3, 0.2]
            )[0]

    def repair_greedy(self, primary_mapping: List[HostId], mirror_mapping: List[HostId],
                   destroyed: Set[ContentId]) -> Tuple[List[HostId], List[HostId]]:
        """
        Greedy: Assign each segment to cheapest valid option immediately.
        O(n * h²) complexity. Fast, reliable.
        """
        new_primary_mapping = primary_mapping[:]
        new_mirror_mapping = mirror_mapping[:]

        # Clear destroyed segments
        for seg in destroyed:
            new_primary_mapping[seg] = -1
            new_mirror_mapping[seg] = -1

        # Initialize load tracker
        primary_capacity = [self.target_load] * self.n_hosts
        mirror_capacity = [self.target_load] * self.n_hosts

        for seg in range(self.n_segments):
            if seg not in destroyed and new_primary_mapping[seg] != -1:
                primary_capacity[new_primary_mapping[seg]] -= 1
                mirror_capacity[new_mirror_mapping[seg]] -= 1

        # Build constraint cache
        if self.strategy == 'grouped':
            primary_to_mirror = {}
            for seg in range(self.n_segments):
                if seg not in destroyed and new_primary_mapping[seg] != -1:
                    primary_to_mirror[new_primary_mapping[seg]] = new_mirror_mapping[seg]
        elif self.strategy == 'spread':
            primary_to_used_mirrors = defaultdict(set)
            for seg in range(self.n_segments):
                if seg not in destroyed and new_primary_mapping[seg] != -1:
                    primary_to_used_mirrors[new_primary_mapping[seg]].add(new_mirror_mapping[seg])

        # Shuffle to avoid bias
        destroyed_list = list(destroyed)
        random.shuffle(destroyed_list)

        for seg in destroyed_list:
            orig_p = self.initial_primary_mapping[seg]
            orig_m = self.initial_mirror_mapping[seg]
            best_cost = float('inf')
            best_p, best_m = -1, -1
            # Try each primary
            for p_host in range(self.n_hosts):
                if primary_capacity[p_host] <= 0:
                    continue
                
                # Get valid mirrors for this primary
                valid_mirrors = self._get_valid_mirrors(
                    p_host,  mirror_capacity,
                    primary_to_mirror if self.strategy == 'grouped' else primary_to_used_mirrors
                )
                if not valid_mirrors:
                    continue
                
                # Calculate primary cost
                p_cost = 0 if p_host == orig_p else 1
                # Try each valid mirror
                for m_host in valid_mirrors:
                    m_cost = 0 if m_host == orig_m else 1
                    total_cost = p_cost + m_cost
                    if total_cost < best_cost:
                        best_cost = total_cost
                        best_p, best_m = p_host, m_host
                        # Early exit on perfect match
                        if total_cost == 0:
                            break
                        
                if best_cost == 0:
                    break
                
            # Assign if found
            if best_p != -1:
                new_primary_mapping[seg] = best_p
                new_mirror_mapping[seg] = best_m
                primary_capacity[best_p] -= 1
                mirror_capacity[best_m] -= 1
                # Update cache
                if self.strategy == 'grouped' and best_p not in primary_to_mirror:
                    primary_to_mirror[best_p] = best_m
                elif self.strategy == 'spread':
                    primary_to_used_mirrors[best_p].add(best_m)

        return new_primary_mapping, new_mirror_mapping

    def _get_valid_mirrors(self, 
                           primary_host: HostId, 
                           mirror_capacity: List[int],
                           constraints_cache: Union[Dict[HostId, HostId], Dict[HostId, Set[HostId]]]) -> Set[HostId]:
        """
        Fast lookup of valid mirror hosts for a given primary host.

        Args:
            primary_host: The primary host we want to assign mirror hostto
            mirror_capacity: Remaining capacity on each mirror host
            constraints_cache: Either:
                - For grouped: Dict[primary -> mirror] mapping
                - For spread: Dict[primary -> Set[used_mirrors]]

        Returns:
            Set of valid mirror host ids that can be used
        """

        if self.strategy == 'grouped':
            # GROUPED STRATEGY: Each primary must use exactly ONE mirror for all segments

            if primary_host in constraints_cache:
                # This primary already has an assigned mirror
                existing_mirror = constraints_cache[primary_host]

                # Can only use this existing mirror if it has capacity
                if mirror_capacity[existing_mirror] > 0:
                    return {existing_mirror}
                else:
                    return set()  # No valid mirrors (existing one is full)

            else:
                # New primary: can use any mirror with capacity (except itself)
                valid = set()
                for m_host in range(self.n_hosts):
                    if m_host != primary_host and mirror_capacity[m_host] > 0:
                        valid.add(m_host)
                return valid

        elif self.strategy == 'spread':
            # SPREAD STRATEGY: Each primary must use DIFFERENT mirrors for each segment

            used_mirrors = constraints_cache.get(primary_host, set())

            valid = set()
            for m_host in range(self.n_hosts):
                if (m_host != primary_host and 
                    m_host not in used_mirrors and 
                    mirror_capacity[m_host] > 0):
                    valid.add(m_host)

            return valid

    def repair_most_constrained(self, primary_mapping: List[HostId], mirror_mapping: List[HostId],
                           destroyed: Set[ContentId]) -> Tuple[List[HostId], List[HostId]]:
        """
        Most Constrained: Assign segments with fewest valid options first.
        Useful for sread strategy.
        O(n**2 * h**2) complexity.
        """
        new_primary_mapping = primary_mapping[:]
        new_mirror_mapping = mirror_mapping[:]

        for seg in destroyed:
            new_primary_mapping[seg] = -1
            new_mirror_mapping[seg] = -1

        primary_capacity = [self.target_load] * self.n_hosts
        mirror_capacity = [self.target_load] * self.n_hosts

        for seg in range(self.n_segments):
            if seg not in destroyed and new_primary_mapping[seg] != -1:
                primary_capacity[new_primary_mapping[seg]] -= 1
                mirror_capacity[new_mirror_mapping[seg]] -= 1

        # Build constraint cache
        if self.strategy == 'grouped':
            primary_to_mirror = {}
            for seg in range(self.n_segments):
                if seg not in destroyed and new_primary_mapping[seg] != -1:
                    primary_to_mirror[new_primary_mapping[seg]] = new_mirror_mapping[seg]
        elif self.strategy == 'spread':
            primary_to_used_mirrors = defaultdict(set)
            for seg in range(self.n_segments):
                if seg not in destroyed and new_primary_mapping[seg] != -1:
                    primary_to_used_mirrors[new_primary_mapping[seg]].add(new_mirror_mapping[seg])

        unassigned = set(destroyed)

        while unassigned:
            # Evaluate ALL unassigned segments
            candidates = []

            for seg in unassigned:
                orig_p = self.initial_primary_mapping[seg]
                orig_m = self.initial_mirror_mapping[seg]

                # Find ALL valid (primary, mirror) pairs for this segment
                valid_options = []

                for p_host in range(self.n_hosts):
                    if primary_capacity[p_host] <= 0:
                        continue
                    
                    # Get valid mirrors dynamically
                    valid_mirrors = self._get_valid_mirrors(
                        p_host, mirror_capacity,
                        primary_to_mirror if self.strategy == 'grouped' else primary_to_used_mirrors
                    )

                    for m_host in valid_mirrors:
                        # Calculate cost for this option
                        cost = (0 if p_host == orig_p else 1) + (0 if m_host == orig_m else 1)
                        valid_options.append((cost, p_host, m_host))

                if not valid_options:
                    # No valid options - this segment is problematic
                    # Add with infinity to handle at end (might fail gracefully)
                    candidates.append((float('inf'), float('inf'), seg, None))
                else:
                    # Sort by cost and pick best
                    valid_options.sort()
                    best_cost, best_p, best_m = valid_options[0]
                    option_count = len(valid_options)

                    # Store: (option_count, best_cost, segment, best_assignment)
                    candidates.append((option_count, best_cost, seg, (best_p, best_m)))

            if not candidates:
                break
            
            # Sort: by option_count (ascending), by cost (ascending)
            # This ensures we assign the hardest segment first
            candidates.sort(key=lambda x: (x[0], x[1]))

            # Process most constrained segment
            option_count, cost, seg, assignment = candidates[0]

            if assignment is None:
                # No valid assignment possible - skip or handle
                unassigned.remove(seg)
                continue
            
            best_p, best_m = assignment

            # Assign
            new_primary_mapping[seg] = best_p
            new_mirror_mapping[seg] = best_m
            primary_capacity[best_p] -= 1
            mirror_capacity[best_m] -= 1

            # Update cache
            if self.strategy == 'grouped' and best_p not in primary_to_mirror:
                primary_to_mirror[best_p] = best_m
            elif self.strategy == 'spread':
                primary_to_used_mirrors[best_p].add(best_m)

            unassigned.remove(seg)

        return new_primary_mapping, new_mirror_mapping

    def repair_regret(self, primary_mapping: List[HostId], mirror_mapping: List[HostId],
                  destroyed: Set[ContentId]) -> Tuple[List[HostId], List[HostId]]:
        """
        Regret: Assign segment with highest regret score (best - 2nd_best) first.
        O(n**2 * h**2) complexity.
        """
        new_primary_mapping = primary_mapping[:]
        new_mirror_mapping = mirror_mapping[:]

        for seg in destroyed:
            new_primary_mapping[seg] = -1
            new_mirror_mapping[seg] = -1

        primary_capacity = [self.target_load] * self.n_hosts
        mirror_capacity = [self.target_load] * self.n_hosts

        for seg in range(self.n_segments):
            if seg not in destroyed and new_primary_mapping[seg] != -1:
                primary_capacity[new_primary_mapping[seg]] -= 1
                mirror_capacity[new_mirror_mapping[seg]] -= 1

        # Build constraint cache
        if self.strategy == 'grouped':
            primary_to_mirror = {}
            for seg in range(self.n_segments):
                if seg not in destroyed and new_primary_mapping[seg] != -1:
                    primary_to_mirror[new_primary_mapping[seg]] = new_mirror_mapping[seg]
        elif self.strategy == 'spread':
            primary_to_used_mirrors = defaultdict(set)
            for seg in range(self.n_segments):
                if seg not in destroyed and new_primary_mapping[seg] != -1:
                    primary_to_used_mirrors[new_primary_mapping[seg]].add(new_mirror_mapping[seg])

        unassigned = list(destroyed)

        while unassigned:
            best_regret = -1
            best_seg = None
            best_assignment = None

            # Calculate regret for each unassigned segment
            for seg in unassigned:
                orig_p = self.initial_primary_mapping[seg]
                orig_m = self.initial_mirror_mapping[seg]

                # Find all valid options
                options = []

                for p_host in range(self.n_hosts):
                    if primary_capacity[p_host] <= 0:
                        continue
                    
                    valid_mirrors = self._get_valid_mirrors(
                        p_host, mirror_capacity,
                        primary_to_mirror if self.strategy == 'grouped' else primary_to_used_mirrors
                    )

                    for m_host in valid_mirrors:
                        cost = (0 if p_host == orig_p else 1) + (0 if m_host == orig_m else 1)
                        options.append((cost, p_host, m_host))

                if len(options) == 0:
                    continue  # Skip infeasible segments
                
                # Sort by cost
                options.sort()

                # Calculate regret
                if len(options) == 1:
                    # Only one option = infinite regret
                    regret = float('inf')
                else:
                    # Regret = difference between best and 2nd best
                    regret = options[1][0] - options[0][0]

                # Track segment with highest regret
                if regret > best_regret:
                    best_regret = regret
                    best_seg = seg
                    best_assignment = options[0]  # (cost, p, m)

            if best_seg is None:
                break
            
            # Assign the segment with highest regret
            _, best_p, best_m = best_assignment

            new_primary_mapping[best_seg] = best_p
            new_mirror_mapping[best_seg] = best_m
            primary_capacity[best_p] -= 1
            mirror_capacity[best_m] -= 1

            # Update cache
            if self.strategy == 'grouped' and best_p not in primary_to_mirror:
                primary_to_mirror[best_p] = best_m
            elif self.strategy == 'spread':
                primary_to_used_mirrors[best_p].add(best_m)

            unassigned.remove(best_seg)

        return new_primary_mapping, new_mirror_mapping


class ALNS:
    """
    Adaptive Large Neighborhood Search (ALNS) for refining segment
    rebalancing solutions produced by GreedySolver.

    Uses:
      - several destroy operators
      - several repair operators
      - simulated annealing for acceptance
      - occasional local search (mirror swaps in grouped)
    """
    def __init__(self,
                 solver: GreedySolver,
                 config: ALNSConfig = None):
        self.solver = solver
        self.config = config or ALNSConfig()

        self.solver = solver
        self.n_segments = solver.n_segments
        self.n_hosts = solver.n_hosts_target
        self.strategy = solver.strategy
        self.initial_primary_mapping = solver.initial_primary_mapping
        self.initial_mirror_mapping = solver.initial_mirror_mapping
        self.target_primary_load = solver.target_primary_load
        self.printing = solver.printing
    
        self.start_time = None

        self.destroy_ops = ALNSDestroyOperators(self.n_segments, self.strategy,
                                                self.initial_primary_mapping, self.initial_mirror_mapping)
        
        self.repair_ops = ALNSRepairOperators(
            self.n_segments, self.n_hosts, self.strategy,
            self.initial_primary_mapping, self.initial_mirror_mapping,
            self.solver.target_primary_load
        )

    def optimize(self, 
                 primary_mapping: List[HostId],
                 mirror_mapping: List[HostId]) -> Tuple[List[HostId], List[HostId]]:
        """
        ALNS.
        Uses all destroy/repair strategies with SA acceptance.
        """
        self.start_time = time.time()

        initial_cost = self.solver._calculate_cost(primary_mapping, mirror_mapping)

        state = SearchState(current_primary_mapping=primary_mapping[:],
                            current_mirror_mapping=mirror_mapping[:],
                            current_cost=initial_cost,
                            best_primary_mapping=primary_mapping[:],
                            best_mirror_mapping=mirror_mapping[:],
                            best_cost=initial_cost)
        
        if self.solver.printing:
            print(f"Initial cost: {state.best_cost}")
        
        for state.iteration in range(self.config.max_iterations):
            if self._timeout_reached():
                if self.printing:
                    print(f"\n Time limit reached ({self.config.timeout}s) at {state.iteration} iteration")
                return state.best_primary_mapping, state.best_mirror_mapping
            
            new_primary_mapping, new_mirror_mapping, destroy_method, repair_method = self._generate_neighbor(state)
            
            # Validate
            if not self._is_valid(new_primary_mapping, new_mirror_mapping):
                state.stagnation_count += 1
                continue

            new_cost = self.solver._calculate_cost(new_primary_mapping, new_mirror_mapping)
            
            # SA acceptance
            if self._accept_move(state.current_cost, new_cost, state.get_temperature(self.config)):
                # Accept move
                state.current_primary_mapping = new_primary_mapping
                state.current_mirror_mapping = new_mirror_mapping
                state.current_cost = new_cost
                state.stagnation_count = 0

                if state.update_best(new_primary_mapping, new_mirror_mapping, new_cost):   
                    if self.printing:
                        print(f"Iter {state.iteration} ({destroy_method.value}) "
                              f"({repair_method.value}): NEW BEST = {state.best_cost}")
            else:
                state.stagnation_count += 1
            
            # Restart from best if stuck
            if state.should_restart(self.config):
                if self.printing:
                    print(f"Iter {state.iteration}: Restarting from best (cost={state.best_cost})")
                state.restart_from_best()
        
        if self.printing:
            print(f"Final cost: {state.best_cost}")
        
        return state.best_primary_mapping, state.best_mirror_mapping

    def _generate_neighbor(self, state: SearchState) -> Tuple[List[HostId], List[HostId],
                                                              ALNSDestroyMethod, ALNSRepairMethod]:
        """
        Generate a neighbor solution using destroy/repair.
        """
        destroy_size = state.get_destroy_size(self.config)
        destroy_method = self.destroy_ops.select_method(state.stagnation_count)
        
        # Apply destroy
        destroyed = self._apply_destroy(state.current_primary_mapping, state.current_mirror_mapping, 
                                      destroy_method, destroy_size)
        
        
        repair_method = self.repair_ops.select_method(state.iteration, self.config.max_iterations)

        new_primary_mapping, new_mirror_mapping = self._apply_repair(state.current_primary_mapping,
                                                     state.current_mirror_mapping,
                                                     repair_method, destroyed)
        
        # Apply local search periodically
        if (self.strategy == 'grouped' and 
            state.stagnation_count < 10 and
            state.iteration % self.config.local_search_frequency == 0):
            new_primary_mapping, new_mirror_mapping = self._local_mirror_swap(new_primary_mapping, new_mirror_mapping)
        
        return new_primary_mapping, new_mirror_mapping, destroy_method, repair_method

    def _apply_destroy(self, primary: List[HostId], mirror: List[HostId],
                      method: ALNSDestroyMethod, destroy_size: float) -> Set[ContentId]:
        """
        Apply the selected destroy method.
        """
        if method == ALNSDestroyMethod.GROUP_DESTROY and self.strategy == 'grouped':
            return self.destroy_ops.destroy_primary_groups(primary, mirror, destroy_size)
        elif method == ALNSDestroyMethod.BAD_SEGMENTS:
            return self.destroy_ops.destroy_bad_segments(primary, mirror, destroy_size)
        elif method == ALNSDestroyMethod.SHAW_REMOVAL:
            return self.destroy_ops.shaw_removal(primary, mirror, destroy_size)
        else:  # RANDOM_SEGMENTS
            return self.destroy_ops.destroy_random(destroy_size)

    # REPAIR
    def _apply_repair(self,
                      primary: List[HostId],
                      mirror: List[HostId],
                      method: ALNSRepairMethod,
                      destroyed: Set[ContentId]) -> Tuple[List[HostId], List[HostId]]:
        if method == ALNSRepairMethod.GREEDY_REPAIR:
            return self.repair_ops.repair_greedy(primary, mirror, destroyed)
        elif method == ALNSRepairMethod.CONSTRAINT_REPAIR:
            return self.repair_ops.repair_most_constrained(primary, mirror, destroyed)
        elif method == ALNSRepairMethod.REGRET_REPAIR:
            return self.repair_ops.repair_regret(primary, mirror, destroyed)

    # LOCAL SEARCH
    def _local_mirror_swap(self, primary_mapping: List[HostId],
                           mirror_mapping: List[HostId]) -> Tuple[List[HostId], List[HostId]]:
        """
        Local search: swap mirror assignments between two primary groups.
        Only for grouped strategy.
        """
        if self.strategy != 'grouped':
            return primary_mapping, mirror_mapping
        
        # Build primary -> mirror mapping
        groups = defaultdict(list)
        ph_to_mh = {}
        
        for seg in range(self.n_segments):
            p = primary_mapping[seg]
            m = mirror_mapping[seg]
            groups[p].append(seg)
            ph_to_mh[p] = m
        
        current_cost = self.solver._calculate_cost(primary_mapping, mirror_mapping)
        best_mirror_mapping = mirror_mapping[:]
        best_cost = current_cost
        
        # Try swapping mirrors between random pairs
        primaries = list(groups.keys())
        
        if len(primaries) < 2:
            return primary_mapping, mirror_mapping
        
        attempts = min(20, len(primaries) * (len(primaries) - 1) // 2)
        
        for _ in range(attempts):
            p1, p2 = random.sample(primaries, 2)
            m1 = ph_to_mh[p1]
            m2 = ph_to_mh[p2]
            
            # Check if swap is valid (doesn't violate primary != mirror)
            if m1 == p2 or m2 == p1:
                continue
            
            # Create candidate solution with swapped mirrors
            candidate_mirror_mapping = mirror_mapping[:]
            for seg in groups[p1]:
                candidate_mirror_mapping[seg] = m2
            for seg in groups[p2]:
                candidate_mirror_mapping[seg] = m1
            
            candidate_cost = self.solver._calculate_cost(primary_mapping, candidate_mirror_mapping)
            
            if candidate_cost < best_cost:
                best_mirror_mapping = candidate_mirror_mapping
                best_cost = candidate_cost
        
        return primary_mapping, best_mirror_mapping

    # UTILITIES
    def _accept_move(self, current_cost: Cost, new_cost: Cost, temperature: float) -> bool:
        """
        Simulated Annealing acceptance criterion.
        """
        if new_cost < current_cost:
            return True
        elif new_cost == current_cost:
            return random.random() < 0.6
        else:
            delta = (new_cost - current_cost) / self.n_segments
            delta_capped = min(delta, 5.0)
            return random.random() < math.exp(-delta_capped / temperature)

    def _is_valid(self, primary_mapping: List[HostId], mirror_mapping: List[HostId]) -> bool:
        """
        Validation check.
        """
        solution = {i: (primary_mapping[i], mirror_mapping[i]) for i in range(self.n_segments)}
        return self.solver._validate_solution(solution)

    def _timeout_reached(self) -> bool:
        """Check if time limit exceeded."""
        if self.start_time is None or self.config.timeout is None:
            return False
        return time.time() - self.start_time > self.config.timeout
