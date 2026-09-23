#!/usr/bin/env python3
"""
Cluster Configuration Generator for Testing

Generates various cluster configurations with different characteristics:
- Balanced vs Unbalanced load distribution
- Grouped vs Spread mirroring strategies
- Various cluster sizes (hosts, segments)
"""

import random
from typing import List, Dict, Tuple, Optional
from dataclasses import dataclass
from enum import Enum
import argparse

class MirroringStrategy(Enum):
    GROUPED = "grouped"
    SPREAD = "spread"

@dataclass
class SegmentConfig:
    """
    Single segment configuration
    """
    dbid: int
    content: int
    role: str  # 'p' or 'm'
    preferred_role: str
    mode: str = 's'
    status: str = 'u'
    hostname: str = ''
    address: str = ''
    port: int = 0
    datadir: str = ''
    
    def to_line(self) -> str:
        """
        Convert to str
        """
        return (f"{self.dbid}|{self.content}|{self.role}|{self.preferred_role}|"
                f"{self.mode}|{self.status}|{self.hostname}|{self.address}|"
                f"{self.port}|{self.datadir}")

class ClusterConfigGenerator:
    """
    Generate Greengage cluster configurations
    """
    
    def __init__(self, n_segments: int, n_hosts: int, strategy: MirroringStrategy,
                 base_primary_port: int = 7000, base_mirror_port: int = 8000):
        self.n_segments = n_segments
        self.n_hosts = n_hosts
        self.strategy = strategy
        self.base_primary_port = base_primary_port
        self.base_mirror_port = base_mirror_port
        self.next_dbid = 2  # Start after coordinator (dbid=1)
    
    def generate_balanced_grouped(self) -> List[SegmentConfig]:
        """
        Generate balanced configuration with GROUPED mirroring.
        
        Pattern: Primaries and mirrors evenly distributed.
        All mirrors for primaries on host_i go to host_((i+1) % n_hosts)
        """
        configs = []
        segments_per_host = self.n_segments // self.n_hosts
        
        if self.n_segments % self.n_hosts != 0:
            raise ValueError(f"Cannot evenly distribute {self.n_segments} segments "
                           f"across {self.n_hosts} hosts for balanced config")
        
        # Assign primaries
        primary_assignment = {}
        content_id = 0
        
        for host_id in range(self.n_hosts):
            for _ in range(segments_per_host):
                primary_assignment[content_id] = host_id
                content_id += 1
        
        # Assign mirrors (grouped)
        mirror_assignment = {}
        for content_id, primary_host in primary_assignment.items():
            mirror_host = (primary_host + 1) % self.n_hosts
            mirror_assignment[content_id] = mirror_host
        
        # Generate segment configs
        configs.extend(self._create_segments(primary_assignment, mirror_assignment))
        
        return configs
    
    def generate_balanced_spread(self) -> List[SegmentConfig]:
        """
        Generate balanced configuration with SPREAD mirroring.
        
        Pattern: Primaries and mirrors evenly distributed.
        Mirrors for primaries on one host are spread across different hosts.
        """
        configs = []
        segments_per_host = self.n_segments // self.n_hosts
        
        if self.n_segments % self.n_hosts != 0:
            raise ValueError(f"Cannot evenly distribute {self.n_segments} segments "
                           f"across {self.n_hosts} hosts for balanced config")
        
        # Assign primaries
        primary_assignment = {}
        content_id = 0

        for host_id in range(self.n_hosts):
            for _ in range(segments_per_host):
                primary_assignment[content_id] = host_id
                content_id += 1

        # Assign mirrors using global offset pattern
        mirror_assignment = {}

        for content_id in range(self.n_segments):
            primary_host = primary_assignment[content_id]

            # Calculate local index within primary host's segments
            local_idx = content_id % segments_per_host

            # Global offset ensures different mirrors for same local_idx across hosts
            # Formula: mirror = (primary + local_idx + global_offset) % n_hosts
            # where global_offset depends on which primary host we're on

            # Use: mirror = (primary + 1 + local_idx) % n_hosts
            # If mirror == primary, shift by 1 again
            mirror_host = (primary_host + 1 + local_idx) % self.n_hosts

            # Ensure no colocation
            if mirror_host == primary_host:
                mirror_host = (mirror_host + 1) % self.n_hosts

            mirror_assignment[content_id] = mirror_host

        configs.extend(self._create_segments(primary_assignment, mirror_assignment))
        
        return configs
    
    def generate_unbalanced_grouped(self, skew_factor: float = 0.2) -> List[SegmentConfig]:
        """
        Generate UNBALANCED configuration with GROUPED mirroring.

        Args:
            skew_factor: 0.0 = perfectly balanced, 1.0 = maximum imbalance
                         Recommended: 0.1-0.3 for realistic scenarios

        Pattern: Some hosts have slightly more segments than others.
        All mirrors for primaries on host_i still go to one host.
        """
        configs = []

        # Create SLIGHTLY skewed distribution (not extreme)
        primary_assignment = self._create_realistic_skewed_distribution(skew_factor)

        # Assign mirrors (grouped)
        mirror_assignment = {}
        mirror_load = [0] * self.n_hosts

        for host_id in range(self.n_hosts):
            # Get all primaries on this host
            primary_contents = [c for c, h in primary_assignment.items() if h == host_id]

            if not primary_contents:
                continue
            
            # Choose mirror host - prefer underloaded hosts for realism
            available_hosts = [h for h in range(self.n_hosts) if h != host_id]

            # Bias towards least loaded mirrors (creates more realistic initial state)
            available_hosts.sort(key=lambda h: mirror_load[h])

            # Pick from top 3 least loaded (adds some randomness)
            target_mirror_host = random.choice(available_hosts[:min(3, len(available_hosts))])

            for content_id in primary_contents:
                mirror_assignment[content_id] = target_mirror_host
                mirror_load[target_mirror_host] += 1

        configs.extend(self._create_segments(primary_assignment, mirror_assignment))

        return configs

    def generate_unbalanced_spread(self, skew_factor: float = 0.2) -> List[SegmentConfig]:
        """
        Generate UNBALANCED configuration with SPREAD mirroring.

        Args:
            skew_factor: 0.0 = perfectly balanced, 1.0 = maximum imbalance
                         Recommended: 0.1-0.3 for realistic scenarios
        """
        configs = []

        # Create slightly skewed distribution
        primary_assignment = self._create_realistic_skewed_distribution(skew_factor)

        # Assign mirrors (spread) with slight preference for balance
        mirror_assignment = {}
        mirror_load = [0] * self.n_hosts

        for host_id in range(self.n_hosts):
            # Get primaries on this host
            primary_contents = [c for c, h in primary_assignment.items() if h == host_id]

            if not primary_contents:
                continue
            
            # Spread mirrors across OTHER hosts, biased towards less loaded
            available_mirror_hosts = [h for h in range(self.n_hosts) if h != host_id]

            for idx, content_id in enumerate(sorted(primary_contents)):
                # Sort by load each time (semi-balanced spread)
                available_mirror_hosts.sort(key=lambda h: (mirror_load[h], h))

                # Pick with some randomness: 70% least loaded, 30% random
                if random.random() < 0.7:
                    mirror_host = available_mirror_hosts[0]  # Least loaded
                else:
                    mirror_host = available_mirror_hosts[idx % len(available_mirror_hosts)]  # Round-robin

                mirror_assignment[content_id] = mirror_host
                mirror_load[mirror_host] += 1

        configs.extend(self._create_segments(primary_assignment, mirror_assignment))

        return configs

    def _create_realistic_skewed_distribution(self, skew_factor: float) -> Dict[int, int]:
        """
        Create realistic skewed distribution.

        Instead of extreme imbalance, this creates a distribution where:
        - Most hosts are within 10-20% of target load
        - A few hosts are overloaded/underloaded
        - Skew_factor controls the magnitude

        Args:
            skew_factor: 0.0 = perfectly balanced, 1.0 = severe imbalance

        Returns:
            Dict[content_id] -> host_id
        """
        target_per_host = self.n_segments / self.n_hosts

        # Calculate actual segments per host with skew
        # Use normal distribution around target with controlled variance
        segments_per_host = []

        for host_id in range(self.n_hosts):
            if skew_factor == 0.0:
                # Perfectly balanced
                count = int(target_per_host)
            else:
                # Add controlled noise
                # skew_factor controls standard deviation
                std_dev = skew_factor * target_per_host * 0.3  # Max 30% deviation
                noise = random.gauss(0, std_dev)
                count = int(target_per_host + noise)

                # Ensure minimum 50% of target (never empty unless skew=1.0)
                min_count = int(target_per_host * max(0.5, 1.0 - skew_factor))
                max_count = int(target_per_host * (1.0 + skew_factor * 0.5))
                count = max(min_count, min(max_count, count))

            segments_per_host.append(count)

        # Adjust to exact total
        current_total = sum(segments_per_host)
        diff = self.n_segments - current_total

        # Distribute difference to random hosts
        while diff != 0:
            host_id = random.randint(0, self.n_hosts - 1)
            if diff > 0:
                segments_per_host[host_id] += 1
                diff -= 1
            elif segments_per_host[host_id] > 1:  # Never go to 0
                segments_per_host[host_id] -= 1
                diff += 1

        # Create assignment
        primary_assignment = {}
        content_id = 0

        for host_id in range(self.n_hosts):
            for _ in range(segments_per_host[host_id]):
                primary_assignment[content_id] = host_id
                content_id += 1

        return primary_assignment
    
    def _create_segments(self, primary_assignment: Dict[int, int],
                        mirror_assignment: Dict[int, int]) -> List[SegmentConfig]:
        """Create SegmentConfig objects from assignments"""
        configs = []
        
        # Create primaries
        for content_id in sorted(primary_assignment.keys()):
            host_id = primary_assignment[content_id]
            hostname = f"sdw{host_id + 1}"
            
            # Count how many primaries on this host before this one
            port_offset = sum(1 for c, h in primary_assignment.items() 
                            if h == host_id and c < content_id)
            
            config = SegmentConfig(
                dbid=self.next_dbid,
                content=content_id,
                role='p',
                preferred_role='p',
                hostname=hostname,
                address=hostname,
                port=self.base_primary_port + content_id,
                datadir=f"/data/primary{content_id}"
            )
            self.next_dbid += 1
            configs.append(config)
        
        # Create mirrors
        for content_id in sorted(mirror_assignment.keys()):
            host_id = mirror_assignment[content_id]
            hostname = f"sdw{host_id + 1}"
            
            config = SegmentConfig(
                dbid=self.next_dbid,
                content=content_id,
                role='m',
                preferred_role='m',
                hostname=hostname,
                address=hostname,
                port=self.base_mirror_port + content_id,
                datadir=f"/data/mirror{content_id}"
            )
            self.next_dbid += 1
            configs.append(config)
        
        return configs
    
    def generate_coordinator(self) -> SegmentConfig:
        """Generate coordinator segment"""
        return SegmentConfig(
            dbid=1,
            content=-1,
            role='p',
            preferred_role='p',
            hostname='cdw',
            address='cdw',
            port=5432,
            datadir='/data/coordinator'
        )
    
    def generate_config(self, balanced: bool = True) -> List[SegmentConfig]:
        """
        Generate complete configuration.
        
        Args:
            balanced: If True, generate balanced config; otherwise unbalanced
        """
        if balanced:
            if self.strategy == MirroringStrategy.GROUPED:
                return self.generate_balanced_grouped()
            else:
                return self.generate_balanced_spread()
        else:
            skew_factor = random.uniform(0.3, 0.8)
            if self.strategy == MirroringStrategy.GROUPED:
                return self.generate_unbalanced_grouped(skew_factor)
            else:
                return self.generate_unbalanced_spread(skew_factor)

def write_config_file(filename: str, configs: List[SegmentConfig], 
                      coordinator: SegmentConfig):
    """Write configuration to file"""
    with open(filename, 'w') as f:
        # Write coordinator
        f.write("# Coordinator\n")
        f.write(coordinator.to_line() + "\n")
        
        # Group segments by host
        segments_by_host = {}
        for config in configs:
            host = config.hostname
            if host not in segments_by_host:
                segments_by_host[host] = []
            segments_by_host[host].append(config)
        
        # Write segments by host
        for hostname in sorted(segments_by_host.keys()):
            f.write(f"# {hostname.upper()}\n")
            
            host_segments = segments_by_host[hostname]
            # Sort by role (primaries first), then by content
            host_segments.sort(key=lambda s: (s.role != 'p', s.content))
            
            for seg in host_segments:
                f.write(seg.to_line() + "\n")

def print_statistics(configs: List[SegmentConfig], n_hosts: int):
    """Print configuration statistics"""
    print("\n" + "=" * 80)
    print("CONFIGURATION STATISTICS".center(80))
    print("=" * 80)
    
    # Count segments per host
    primaries_per_host = [0] * n_hosts
    mirrors_per_host = [0] * n_hosts
    
    for config in configs:
        host_id = int(config.hostname.replace('sdw', '')) - 1
        if config.role == 'p':
            primaries_per_host[host_id] += 1
        else:
            mirrors_per_host[host_id] += 1
    
    print(f"\nTotal Segments: {len(configs) // 2}")
    print(f"Total Hosts: {n_hosts}")
    print(f"\nLoad Distribution:")
    print(f"{'Host':<10} {'Primaries':<12} {'Mirrors':<12} {'Total':<10}")
    print("-" * 50)
    
    for host_id in range(n_hosts):
        hostname = f"sdw{host_id + 1}"
        primaries = primaries_per_host[host_id]
        mirrors = mirrors_per_host[host_id]
        total = primaries + mirrors
        print(f"{hostname:<10} {primaries:<12} {mirrors:<12} {total:<10}")
    
    print("-" * 50)
    
    # Calculate balance metrics
    total_loads = [p + m for p, m in zip(primaries_per_host, mirrors_per_host)]
    avg_load = sum(total_loads) / n_hosts
    max_load = max(total_loads)
    min_load = min(total_loads)
    
    print(f"\nBalance Metrics:")
    print(f"  Average load per host: {avg_load:.2f}")
    print(f"  Min load: {min_load}")
    print(f"  Max load: {max_load}")
    print(f"  Load deviation: {max_load - min_load}")
    print(f"  Balance ratio: {min_load / max_load * 100:.1f}%")
    
    # Check mirroring strategy
    print(f"\nMirroring Analysis:")
    
    for host_id in range(n_hosts):
        # Get primaries on this host
        primary_contents = [c.content for c in configs 
                          if c.role == 'p' and c.hostname == f"sdw{host_id + 1}"]
        
        if not primary_contents:
            continue
        
        # Find where their mirrors are
        mirror_hosts = []
        for content in primary_contents:
            mirror = next(c for c in configs if c.role == 'm' and c.content == content)
            mirror_hosts.append(mirror.hostname)
        
        unique_mirror_hosts = len(set(mirror_hosts))
        
        if unique_mirror_hosts == 1:
            strategy = "GROUPED"
        elif unique_mirror_hosts == len(mirror_hosts):
            strategy = "SPREAD (perfect)"
        else:
            strategy = f"SPREAD (partial - {unique_mirror_hosts} hosts)"
        
        print(f"  sdw{host_id + 1}: {len(primary_contents)} primaries → "
              f"mirrors on {unique_mirror_hosts} host(s) [{strategy}]")
    
    print("=" * 80 + "\n")

def main():
    parser = argparse.ArgumentParser(
        description='Generate Greengage cluster test configurations'
    )
    parser.add_argument('-n', '--segments', type=int, required=True,
                       help='Number of segment pairs')
    parser.add_argument('-m', '--hosts', type=int, required=True,
                       help='Number of hosts')
    parser.add_argument('-s', '--strategy', choices=['grouped', 'spread'],
                       required=True, help='Mirroring strategy')
    parser.add_argument('-b', '--balanced', action='store_true',
                       help='Generate balanced configuration')
    parser.add_argument('-u', '--unbalanced', action='store_true',
                       help='Generate unbalanced configuration')
    parser.add_argument('-o', '--output', type=str, default='cluster_config.txt',
                       help='Output filename')
    parser.add_argument('--seed', type=int, default=None,
                       help='Random seed for reproducibility')
    
    args = parser.parse_args()
    
    # Set random seed
    if args.seed is not None:
        random.seed(args.seed)
    
    # Determine balance
    if args.balanced and args.unbalanced:
        print("Error: Cannot specify both --balanced and --unbalanced")
        return
    
    balanced = args.balanced or not args.unbalanced
    
    # Create generator
    strategy = MirroringStrategy.GROUPED if args.strategy == 'grouped' else MirroringStrategy.SPREAD
    
    generator = ClusterConfigGenerator(
        n_segments=args.segments,
        n_hosts=args.hosts,
        strategy=strategy
    )
    
    # Generate configuration
    print(f"Generating {'balanced' if balanced else 'unbalanced'} configuration...")
    print(f"  Segments: {args.segments}")
    print(f"  Hosts: {args.hosts}")
    print(f"  Strategy: {args.strategy.upper()}")
    
    try:
        configs = generator.generate_config(balanced=balanced)
        coordinator = generator.generate_coordinator()
        
        # Write to file
        write_config_file(args.output, configs, coordinator)
        print(f"\nConfiguration written to: {args.output}")
        
        # Print statistics
        print_statistics(configs, args.hosts)
        
    except ValueError as e:
        print(f"Error: {e}")
        return

# Batch generator for test suite
def generate_test_suite():
    """Generate a comprehensive test suite"""
    
    test_cases = [
        # Small balanced configs
        {'n': 35, 'm': 7, 'strategy': 'grouped', 'balanced': True, 'name': '35_7_balanced_grouped'},
        {'n': 35, 'm': 7, 'strategy': 'spread', 'balanced': True, 'name': '35_7_balanced_spread'},
        
        # Small unbalanced configs
        {'n': 40, 'm': 5, 'strategy': 'grouped', 'balanced': False, 'name': '40_5_unbalanced_grouped'},
        {'n': 40, 'm': 5, 'strategy': 'spread', 'balanced': False, 'name': '40_5_unbalanced_spread'},
        
        # Medium balanced configs
        {'n': 120, 'm': 20, 'strategy': 'grouped', 'balanced': True, 'name': '120_20_balanced_grouped'},
        {'n': 120, 'm': 20, 'strategy': 'spread', 'balanced': True, 'name': '120_20_balanced_spread'},
        
        # Medium unbalanced configs
        {'n': 120, 'm': 20, 'strategy': 'grouped', 'balanced': False, 'name': '120_20_unbalanced_grouped'},
        {'n': 120, 'm': 20, 'strategy': 'spread', 'balanced': False, 'name': '120_20_unbalanced_spread'},
        
        # Large balanced configs
        {'n': 1000, 'm': 50, 'strategy': 'grouped', 'balanced': True, 'name': '1000_50_balanced_grouped'},
        {'n': 1000, 'm': 50, 'strategy': 'spread', 'balanced': True, 'name': '1000_50_balanced_spread'},
        
        # Large unbalanced configs
        {'n': 1000, 'm': 50, 'strategy': 'grouped', 'balanced': False, 'name': '1000_50_unbalanced_grouped'},
        {'n': 1000, 'm': 50, 'strategy': 'spread', 'balanced': False, 'name': '1000_50_unbalanced_spread'},
    ]
    
    print("=" * 80)
    print("GENERATING TEST SUITE".center(80))
    print("=" * 80)
    
    for idx, test_case in enumerate(test_cases, 1):
        print(f"\n[{idx}/{len(test_cases)}] Generating: {test_case['name']}")
        
        strategy = MirroringStrategy.GROUPED if test_case['strategy'] == 'grouped' else MirroringStrategy.SPREAD
        
        generator = ClusterConfigGenerator(
            n_segments=test_case['n'],
            n_hosts=test_case['m'],
            strategy=strategy
        )
        
        try:
            configs = generator.generate_config(balanced=test_case['balanced'])
            coordinator = generator.generate_coordinator()
            
            filename = f"{test_case['name']}.array"
            write_config_file(filename, configs, coordinator)
            
            print(f"  ✓ Written to: {filename}")
            print_statistics(configs, test_case['m'])
            
        except Exception as e:
            print(f"  ✗ Failed: {e}")
    
    print("\n" + "=" * 80)
    print("TEST SUITE GENERATION COMPLETE".center(80))
    print("=" * 80)

if __name__ == '__main__':
    import sys
    
    if len(sys.argv) > 1 and sys.argv[1] == '--generate-suite':
        generate_test_suite()
    elif len(sys.argv) == 1:
        # Interactive mode or show help
        print("Usage:")
        print("  Generate single config:")
        print("    python config_generator.py -n 6 -m 3 -s grouped -b -o config.txt")
        print("\n  Generate test suite:")
        print("    python config_generator.py --generate-suite")
        print("\n  For help:")
        print("    python config_generator.py -h")
    else:
        main()
