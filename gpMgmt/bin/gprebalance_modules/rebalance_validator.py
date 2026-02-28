from typing import List
from gprebalance_modules.rebalance import Host, MirrorStrategy, dbconn, GpArray,  Segment, MODE_NOT_SYNC, STATUS_DOWN


class StateValidationError(Exception):
    pass


class ClusterValidator:
    def __init__(self, existing_hosts: List[Host], target_hosts: List[Host], segarray: List[Segment], has_mirrors: bool, mirror_strategy: MirrorStrategy):
        self.mirror_strategy = mirror_strategy
        self.existing_hosts = existing_hosts
        self.target_hosts = target_hosts
        self.segarray = segarray
        self.has_mirrors = has_mirrors

    def validate_segment_status(self):
        inv = [seg.content for seg in self.segarray if not seg.valid]
        if len(inv) > 0:
            raise StateValidationError(
                f"The {[c for c in inv]} segments are down")

    def validate_existing_configuration(self) -> tuple[bool, MirrorStrategy]:
        arr = GpArray(self.segarray)
        total_primaries = arr.get_primary_count()
        total_hosts = len(self.existing_hosts)
        expected_primaries = total_primaries // total_hosts
        strat = None

        if arr.guessIsSpreadMirror():
            strat = MirrorStrategy.SPREAD
        elif arr.hasMirrors:
            strat = MirrorStrategy.GROUPED
            for host in self.existing_hosts:
                if host.primary_segments & host.mirror_segments:
                    strat = None
                    break

        for host in self.existing_hosts:
            if len(host.primary_segments) != expected_primaries:
                return False, strat

        return True, strat

    def prevalidate_segment_distribution(self):
        """
        Validate whether segments can be uniformly distributed across target hosts
        """
        total_primary_segments = sum(len(h.primary_segments)
                                     for h in self.existing_hosts)
        total_hosts = len(self.target_hosts)

        if total_primary_segments % total_hosts != 0:
            raise StateValidationError(
                f"Cannot evenly distribute {total_primary_segments} segments across {total_hosts} hosts."
            )

    def prevalidate_mirror_strategy(self):
        """
        Validate whether the specified mirroring strategy can be achieved
        """
        if not self.has_mirrors:
            return
        total_hosts = len(self.target_hosts)
        total_primary_segments = sum(len(h.primary_segments)
                                     for h in self.existing_hosts)
        if total_hosts < 2:
            raise StateValidationError(
                """Cannot support target mirroring strategy on given configuration. All
                primaries will be at single host."""
            )

        primaries_per_host = total_primary_segments // total_hosts
        if self.mirror_strategy == MirrorStrategy.SPREAD and primaries_per_host >= total_hosts:
            raise StateValidationError(
                "Cannot support spread mirroring strategy on given configuration. "
                "Use cluster utilities like gpresize or gpexpand to get desired cluster configuration"
            )
