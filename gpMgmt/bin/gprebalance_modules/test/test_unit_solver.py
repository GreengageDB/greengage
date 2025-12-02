import random
from typing import List, Dict, Tuple
from collections import defaultdict

from gppylib.test.unit.gp_unittest import *
from ..solver import GreedySolver, Solution, SolverConfig
from .config import getEncoding


class TestGreedySolver(GpTestCase):

    def setUp(self):
        random.seed(42)
    
    def _validate_solution_allassign(self, solution: Solution, 
                                     solver: GreedySolver) -> bool:
        if len(solution) != solver.n_segments:
            return False

        return True    
    def _validate_solution_host_ids(self, solution: Solution, 
                                     solver: GreedySolver) -> bool:
        for seg_id, (primary, mirror) in solution.items():
            if not (0 <= primary < solver.n_hosts_target):
                return False
            if not (0 <= mirror < solver.n_hosts_target):
                return False
        
        return True
    
    def _validate_solution_nocolocation(self, solution: Solution, 
                                     solver: GreedySolver) -> bool:
        for seg_id, (primary, mirror) in solution.items():
            if primary == mirror:
                return False
        return True
    
    def _validate_solution_balance(self, solution: Solution, 
                                     solver: GreedySolver) -> bool:
        load = [0] * solver.n_hosts_target
        for seg_id, (primary, mirror) in solution.items():
            load[primary] += 1
            load[mirror] += 1
        
        for host, host_load in enumerate(load):
            if host_load != solver.target_load:
                return False
        
        return True
    
    def _validate_strategy(self, solution: Solution, 
                                     solver: GreedySolver):
        primary_to_mirrors = defaultdict(list)
        for seg_id, (primary, mirror) in solution.items():
            primary_to_mirrors[primary].append(mirror)
        
        for primary, mirrors in primary_to_mirrors.items():
            if solver.strategy == 'grouped':
                unique_mirrors = set(mirrors)
                if len(unique_mirrors) > 1:
                    return False
            elif solver.strategy == 'spread':
                unique_mirrors = set(mirrors)
                if len(mirrors) != len(unique_mirrors):
                    return False
        return True
    
    def _validate_solition(self, solution: Solution, 
                                     solver: GreedySolver):
        self.assertTrue(self._validate_solution_allassign(solution, solver),
                        f"Missing segments. Expected {solver.n_segments}, got {len(solution)}")
        
        self.assertTrue(self._validate_solution_host_ids(solution, solver),
                        "Some segments has impossible host assignment")

        self.assertTrue(self._validate_solution_nocolocation(solution, solver),
                        "Some segment has primary==mirror assignment")
        
        self.assertTrue(self._validate_solution_balance(solution, solver),
                        f"One of hosts has load different from expected {solver.target_load}")
        
        self.assertTrue(self._validate_strategy(solution, solver),
                        "Grouped mirroring strategy is violated")

    def perform_run(self, run_improve:bool, cost:int):
        conf = self.encoding[0]
        solver = GreedySolver(conf, run_improve=run_improve)
        solution, actual_cost = solver.solve()
        self.assertEqual(actual_cost, cost)
        self._validate_solition(solution, solver)

    @getEncoding('35_7_balanced_grouped', 'grouped', None, None, None)
    def test_validity_small_grouped_balanced(self):
        self.perform_run(False, 0)
    
    @getEncoding('35_7_balanced_spread', 'spread', None, None, None)
    def test_validity_small_spread_balanced(self):
        self.perform_run(False, 0)

    @getEncoding('40_5_unbalanced_grouped', 'grouped', None, None, None)
    def test_validity_small_grouped_unbalanced(self):
        self.perform_run(False, 19)
    
    @getEncoding('40_5_unbalanced_grouped', 'grouped', None, None, None)
    def test_validity_small_grouped_unbalanced_with_improve(self):
        self.perform_run(True, 18)

    @getEncoding('40_5_unbalanced_spread', 'spread', None, None, None)
    def test_validity_small_spread_neg(self):
        conf = self.encoding[0]

        with self.assertRaises(ValueError, msg='Cannot follow spread mirroring strategy') as cm:
            solver = GreedySolver(conf, run_improve=False)
    
    @getEncoding('40_8_unbalanced_spread', 'spread', None, None, None)
    def test_validity_small_spread_unbalanced(self):
        self.perform_run(False, 10)
    
    @getEncoding('40_8_unbalanced_spread', 'spread', None, None, None)
    def test_validity_small_spread_unbalanced_with_improve(self):
        self.perform_run(True, 6)
    
    @getEncoding('120_20_unbalanced_spread', 'spread', None, None, None)
    def test_validity_medium_spread_unbalanced(self):
        self.perform_run(False, 10)
    
    @getEncoding('120_20_unbalanced_spread', 'spread', None, None, None)
    def test_validity_medium_spread_unbalanced_with_improve(self):
        self.perform_run(True, 7)

    @getEncoding('1000_50_unbalanced_spread', 'spread', None, None, None)
    def test_validity_large_spread_unbalanced(self):
       self.perform_run(False, 124)
    
    @getEncoding('1000_50_unbalanced_spread', 'spread', None, None, None)
    def test_validity_large_spread_unbalanced_with_improve(self):
        # in more or less standart configurations with lightly skewed
        # distribution greedy initial solution is pretty-well generated.
        # it's expected that ALNS may not bring any impovements.
        self.perform_run(True, 124)

    @getEncoding('1000_50_unbalanced_grouped', 'grouped', None, None, None)
    def test_validity_large_grouped_unbalanced(self):
        self.perform_run(False, 140)
    
    @getEncoding('1000_50_unbalanced_grouped', 'grouped', None, None, None)
    def test_validity_large_grouped_unbalanced_with_improve(self):
        # in standart configurations with lightly skewed
        # distribution greedy initial solution is pretty-well generated.
        # it's expected that ALNS may not bring any impovements.
        self.perform_run(True, 140)
    
    @getEncoding('120_20_unbalanced_grouped', 'spread', None, None, None)
    def test_strategy_change_medium_grouped_unbalanced(self):
        self.perform_run(False, 101)
    
    @getEncoding('120_20_unbalanced_grouped', 'spread', None, None, None)
    def test_strategy_change_medium_grouped_unbalanced_with_improve(self):
        self.perform_run(True, 100)
    
    @getEncoding('120_20_unbalanced_spread', 'grouped', None, None, None)
    def test_strategy_change_medium_spread_unbalanced(self):
        self.perform_run(False, 106)
    
    @getEncoding('120_20_unbalanced_spread', 'grouped', None, None, None)
    def test_strategy_change_medium_spread_unbalanced_with_improve(self):
       self.perform_run(True, 102)
    
    @getEncoding('120_20_unbalanced_grouped', 'grouped', target_hosts=None,
                 add_hosts=None, remove_hosts="sdw13, sdw14, sdw15, sdw16, sdw17, sdw18, sdw19, sdw20")
    def test_decomission_hosts(self):
        self.perform_run(False, 116)

    @getEncoding('120_20_unbalanced_grouped', 'grouped', target_hosts=None,
                 add_hosts=None, remove_hosts="sdw13, sdw14, sdw15, sdw16, sdw17, sdw18, sdw19, sdw20")
    def test_decomission_hosts_with_improve(self):
        self.perform_run(True, 109)
    
    @getEncoding('1000_50_unbalanced_grouped', 'grouped', target_hosts=None,
                 add_hosts=None, remove_hosts=",".join(["sdw" + str(i) for i in range(20, 30)]))
    def test_decomission_hosts_large(self):
        self.perform_run(False, 470)

    @getEncoding('1000_50_unbalanced_grouped', 'grouped', target_hosts=None,
                 add_hosts=None, remove_hosts=",".join(["sdw" + str(i) for i in range(20, 30)]))
    def test_decomission_hosts_large_with_improve(self):
        self.perform_run(True, 457)
    
    @getEncoding('1000_50_balanced_grouped', 'grouped', target_hosts=None,
    add_hosts=",".join(["sdw" + str(i) for i in range(51, 101)]),
    remove_hosts=None)
    def test_new_hosts_balanced(self):
        self.perform_run(False, 1000)
    
    @getEncoding('1000_50_balanced_grouped', 'grouped', target_hosts=None,
    add_hosts=",".join(["sdw" + str(i) for i in range(51, 101)]),
    remove_hosts=None)
    def test_new_hosts_balanced_with_improve(self):
        self.perform_run(True, 1000)
    
    @getEncoding('120_20_unbalanced_grouped', 'grouped', target_hosts=
                 "sdw1, sdw2, sdw3, sdw4, sdw5, sdw21, sdw22, sdw23, sdw24, sdw25, sdw12, sdw13",
                 add_hosts=None, remove_hosts=None)
    def test_target_hosts(self):
        self.perform_run(False, 175)
    
    @getEncoding('120_20_unbalanced_grouped', 'grouped', target_hosts=
                 "sdw1, sdw2, sdw3, sdw4, sdw5, sdw21, sdw22, sdw23, sdw24, sdw25, sdw12, sdw13",
                 add_hosts=None, remove_hosts=None)
    def test_target_hosts_with_improve(self):
        self.perform_run(True, 160)

class TestGreedySolverFunc(GpTestCase):
    """
    Test GreedySolver class.
    """
    
    def test_init_single_host_error(self):
        config = SolverConfig(
            n_segments=4,
            n_hosts_target=1,  # Invalid!
            n_hosts_initial=2,
            initial_primary_mapping=[0, 0, 1, 1],
            initial_mirror_mapping=[1, 1, 0, 0],
            strategy='grouped'
        )
        with self.assertRaises(ValueError) as ctx:
            GreedySolver(config, run_improve=False)
        self.assertIn("Cannot balance to single host", str(ctx.exception))

    def test_init_uneven_distribution_error(self):
        config = SolverConfig(
            n_segments=13,
            n_hosts_target=3,
            n_hosts_initial=3,
            initial_primary_mapping=[0]*13,
            initial_mirror_mapping=[1]*13,
            strategy='grouped'
        )
        with self.assertRaises(ValueError) as ctx:
            GreedySolver(config, run_improve=False)
        self.assertIn("Cannot evenly distribute", str(ctx.exception))
    
    def test_init_spread_impossible_error(self):
        config = SolverConfig(
            n_segments=12,  # 12/3 = 4 per host, but spread needs max 2 (3-1)
            n_hosts_target=3,
            n_hosts_initial=3,
            initial_primary_mapping=[0]*12,
            initial_mirror_mapping=[1]*12,
            strategy='spread'
        )
        with self.assertRaises(ValueError) as ctx:
            GreedySolver(config, run_improve=False)
        self.assertIn("Cannot follow spread mirroring strategy", str(ctx.exception))
    
    def test_balance_primaries_keep_on_original(self):
        config = SolverConfig(
            n_segments=6,
            n_hosts_target=3,
            n_hosts_initial=3,
            initial_primary_mapping=[0, 0, 1, 1, 2, 2], # Already balanced
            initial_mirror_mapping=[1, 1, 2, 2, 0, 0],
            strategy='grouped'
        )
        solver = GreedySolver(config, run_improve=False)
        primary = solver._balance_primaries()
        
        # Should keep original placements
        self.assertEqual(primary, [0, 0, 1, 1, 2, 2])

    def test_balance_primaries_move_from_decommissioned(self):
        """
        Test that _balance_primaries keeps valid placements.
        """
        config_grouped = SolverConfig(
            n_segments=12,
            n_hosts_target=3,
            n_hosts_initial=4,
            initial_primary_mapping=[0, 1, 2, 0, 1, 2, 0, 1, 2, 3, 3, 3],
            initial_mirror_mapping=[1, 2, 0, 1, 2, 0, 1, 2, 0, 0, 1, 2],
            strategy='grouped'
        )

        solver = GreedySolver(config_grouped, run_improve=False)
        primary_mapping = solver._balance_primaries()
        
        # Check all segments assigned
        self.assertEqual(len(primary_mapping), 12)
        self.assertTrue(all( p in [0, 1, 2]
                            for p in primary_mapping))
    
    def test_balance_primaries_move_from_overloaded(self):
        config = SolverConfig(
            n_segments=12,
            n_hosts_target=3,
            n_hosts_initial=3,
            initial_primary_mapping=[0]*8 + [1]*2 + [2]*2, # Host 0 overloaded
            initial_mirror_mapping=[1]*8 + [2]*2 + [0]*2,
            strategy='grouped'
        )
        solver = GreedySolver(config, run_improve=False)
        primary = solver._balance_primaries()
        
        # Should balance to 4 per host
        primary_count = defaultdict(int)
        for p in primary:
            primary_count[p] += 1
        
        for h in range(3):
            self.assertEqual(primary_count[h], 4)
    
    def test_assign_mirrors_grouped_strategy_balanced(self):
        config = SolverConfig(
            n_segments=12,
            n_hosts_target=3,
            n_hosts_initial=3,
            initial_primary_mapping=[0]*4 + [1]*4 + [2]*4,
            initial_mirror_mapping=[1]*4 + [2]*4 + [0]*4,
            strategy='grouped'
        )
        solver = GreedySolver(config, run_improve=False)
        primary = solver._balance_primaries()
        mirror = solver._assign_mirror_hosts(primary)

        self.assertEqual(primary, config.initial_primary_mapping)
        self.assertEqual(mirror, config.initial_mirror_mapping)
    
    def test_assign_mirrors_spread_strategy_balanced(self):
        config = SolverConfig(
            n_segments=8,
            n_hosts_target=4,
            n_hosts_initial=4,
            initial_primary_mapping=[0, 0, 1, 1, 2, 2, 3, 3],
            initial_mirror_mapping=[1, 2, 2, 3, 3, 0, 0, 1],
            strategy='spread'
        )
        solver = GreedySolver(config, run_improve=False)
        primary = solver._balance_primaries()
        mirror = solver._assign_mirror_hosts(primary)
        
        self.assertEqual(primary, config.initial_primary_mapping)
        self.assertEqual(mirror, config.initial_mirror_mapping)
    
    def test_select_group_mirror_priority1_most_used(self):
        """ most-used original mirror with capacity"""
        config = SolverConfig(
            n_segments=12,
            n_hosts_target=3,
            n_hosts_initial=3,
            initial_primary_mapping=[0, 0, 0, 0, 0, 2, 2, 2, 2, 1, 1, 1],
            initial_mirror_mapping=[1, 1, 1, 2, 2, 1, 1, 2, 2, 0, 0, 0],
            strategy='grouped'
        )
        solver = GreedySolver(config, run_improve=False)
        
        mirror_mapping = [-1] * 12
        mirror_load = [0] * 3
        groups = {0: [0, 1, 2, 3], 1: [4, 5, 6, 7], 2: [8, 9, 10, 11]}
        phost_to_mhost = {}
        
        preferences = solver._compute_mirror_preferences(groups)
        
        best = solver._select_group_mirror(
            p_host=0,
            mirror_mapping=mirror_mapping,
            mirror_load=mirror_load,
            phost_to_mhost=phost_to_mhost,
            groups=groups,
            preferences=preferences
        )
        
        # Should select most-preferred mirror (host 1)
        self.assertEqual(best, 1)
    
    def test_select_group_mirror_priority2_least_loaded(self):
        config = SolverConfig(
            n_segments=12,
            n_hosts_target=3,
            n_hosts_initial=4, # Host 3 decommissioned
            initial_primary_mapping=[0]*4 + [1]*4 + [2]*4,
            initial_mirror_mapping=[3]*12,  # All on decommissioned host
            strategy='grouped'
        )
        solver = GreedySolver(config, run_improve=False)
        
        mirror_mapping = [-1] * 12
        mirror_load = [2, 0, 1] # Host 1 is least loaded
        groups = {0: [0, 1, 2, 3], 1: [4, 5, 6, 7], 2: [8, 9, 10, 11]}
        phost_to_mhost = {}
        
        preferences = solver._compute_mirror_preferences(groups)
        
        best = solver._select_group_mirror(
            p_host=0,
            mirror_mapping=mirror_mapping,
            mirror_load=mirror_load,
            phost_to_mhost=phost_to_mhost,
            groups=groups,
            preferences=preferences
        )
        
        # Should select least loaded (host 1)
        self.assertEqual(best, 1)
    
    def test_select_group_mirror_priority3_deadlock_swap(self):
        config = SolverConfig(
            n_segments=8,
            n_hosts_target=4,
            n_hosts_initial=4,
            initial_primary_mapping=[0]*2 + [1]*2 + [2]*2 + [3]*2,# does not matter
            initial_mirror_mapping=[1]*4 + [0]*4,# does not matter
            strategy='grouped'
        )
        solver = GreedySolver(config, run_improve=False)
        
        # Simulate deadlock: all mirror hosts are full, except the host 0.
        # Need swap in case of such mapping where primary group at host 0
        # has no options to put the mirrors
        mirror_mapping = [2, 2, 3, 3, 1, 1, -1, -1]
        mirror_load = [0, 2, 2, 2]  # Both full
        groups = {0: [6, 7], 1: [0, 1], 2: [2, 3], 3: [4, 5]}
        phost_to_mhost = {1: 2, 2: 3, 3: 1}
                
        best = solver._swap_to_resolve_deadlock(
            blocked_p_host=0,
            mirror_load=mirror_load,
            phost_to_mhost=phost_to_mhost,
            groups=groups,
            mirror_mapping=mirror_mapping
        )
        
        # This tests the deadlock branch is reached
        self.assertIsNotNone(best)
    
    def test_spread_use_original_mirror(self):
        config = SolverConfig(
            n_segments=8,
            n_hosts_target=4,
            n_hosts_initial=4,
            initial_primary_mapping=[0, 0, 1, 1, 2, 2, 3, 3],
            initial_mirror_mapping=[1, 2, 2, 3, 3, 0, 0, 1],  # Valid originals
            strategy='spread'
        )
        solver = GreedySolver(config, run_improve=False)
        primary_mapping = solver._balance_primaries()
        mirror_mapping = solver._assign_mirror_hosts(primary_mapping)

        self.assertEqual(primary_mapping, config.initial_primary_mapping)
        self.assertEqual(mirror_mapping, config.initial_mirror_mapping)
    
    def test_spread_find_alternative(self):
        config = SolverConfig(
            n_segments=8,
            n_hosts_target=4,
            n_hosts_initial=5,
            initial_primary_mapping=[0, 0, 1, 1, 2, 2, 3, 3],
            initial_mirror_mapping=[4, 4, 4, 4, 4, 4, 4, 4],  # All on decommissioned
            strategy='spread'
        )
        solver = GreedySolver(config, run_improve=False)
        primary_mapping = solver._balance_primaries()
        mirror_mapping = solver._assign_mirror_hosts(primary_mapping)
        
        # Should assign to valid hosts
        for m in mirror_mapping:
            self.assertLess(m, 4)
    
    def test_spread_deadlock_swap_found(self):
        config = SolverConfig(
            n_segments=8,
            n_hosts_target=4,
            n_hosts_initial=5,
            initial_primary_mapping=[0, 0, 1, 1, 2, 2, 3, 3],
            initial_mirror_mapping=[4, 4, 4, 4, 4, 4, 4, 4],# All on decommissioned
            strategy='spread'
        )
        solver = GreedySolver(config, run_improve=False)
        
        primary_mapping = [0, 0, 1, 1, 2, 2, 3, 3]
        mirror_mapping = [1, 2, 2, 1, 3, 0, 0, -1]
        mirror_load = [2, 2, 2, 1]
        primary_host_to_mirror_hosts = {
            0: {1, 2},
            1: {1, 2},
            2: {3, 0},
            3: {0}
        }
        
        result = solver._resolve_spread_deadlock_for_segment(
            seg=7,
            primary_mapping=primary_mapping,
            mirror_mapping=mirror_mapping,
            mirror_load=mirror_load,
            used_in_group=primary_host_to_mirror_hosts
        )
        
        # Must find a swap
        self.assertEqual(1, result)
    
if __name__ == '__main__':
    run_tests()
