#
# Copyright (c) 2025-Present, Greengage Community
#

from gppylib.test.unit.gp_unittest import *
from mock import *

from ggrebalance_modules.ggrebalance_main_sm import GGRebalanceMainSM


class GGRebalanceMainSMShutdownTestCase(GpTestCase):
    def setUp(self):
        self.apply_patches([
            patch('ggrebalance_modules.ggrebalance_main_sm.RebalanceSchema'),
            patch('ggrebalance_modules.ggrebalance_main_sm.GGShrink'),
            patch('ggrebalance_modules.ggrebalance_main_sm.RebalanceSM'),
        ])
        self.logger = Mock()
        self.main_sm = GGRebalanceMainSM(Mock(), self.logger, Mock(), Mock(), Mock(), Mock(), 'gparraydump')

    def _set_state(self, state):
        self.main_sm.machine.set_state(state)

    def test_shutdown_exits_before_sub_state_machines_are_started(self):
        for state in GGRebalanceMainSM.states_interruptible:
            self._set_state(state)
            with self.assertRaises(SystemExit) as cm:
                self.main_sm.shutdown()
            self.assertEqual(cm.exception.code, 1)

        self.main_sm.gg_shrink.shutdown.assert_not_called()
        self.main_sm.gg_rebalance.shutdown.assert_not_called()

    def test_soft_shutdown_exits_before_sub_state_machines_are_started(self):
        # the time limit (-T / -E) case
        self.main_sm.set_hard_shutdown(False)
        self._set_state('STATE_PLANNING_STARTED')
        with self.assertRaises(SystemExit) as cm:
            self.main_sm.shutdown()
        self.assertEqual(cm.exception.code, 1)

    def test_shutdown_is_delegated_to_sub_state_machines(self):
        all_states = GGRebalanceMainSM.states_not_logged + GGRebalanceMainSM.states_logged
        delegated_states = [s for s in all_states if s not in GGRebalanceMainSM.states_interruptible]
        self.assertIn('STATE_SETUP_SCHEMA_STARTED', delegated_states)
        self.assertIn('STATE_SHRINK_STARTED', delegated_states)
        self.assertIn('STATE_REBALANCE_STARTED', delegated_states)
        self.assertIn('STATE_ROLLBACK', delegated_states)
        self.assertIn('STATE_CLEANUP', delegated_states)

        for state in delegated_states:
            self.main_sm.gg_shrink.shutdown.reset_mock()
            self.main_sm.gg_rebalance.shutdown.reset_mock()
            self._set_state(state)
            self.main_sm.shutdown()
            self.main_sm.gg_shrink.shutdown.assert_called_once_with()
            self.main_sm.gg_rebalance.shutdown.assert_called_once_with(True)

    def test_interrupted_planning_does_not_create_schema(self):
        # SIGINT arrives while the planner is running
        def interrupted_plan():
            self.main_sm.shutdown()
            return Mock()

        self.main_sm.rebalance_schema.getMainStateFromPreviousRun.return_value = None
        self.main_sm.options.clean_required = False
        self.main_sm.options.rollback_required = False
        self.main_sm.options.target_segment_count = 2
        self.main_sm.options.show_plan = False
        with patch('ggrebalance_modules.ggrebalance_main_sm.Planner') as planner:
            planner.return_value.plan.side_effect = interrupted_plan
            with self.assertRaises(SystemExit):
                self.main_sm.run()

        self.assertEqual(self.main_sm.state, 'STATE_PLANNING_STARTED')
        self.main_sm.rebalance_schema.createSchema.assert_not_called()
        self.main_sm.rebalance_schema.storeMainState.assert_called_once_with('STATE_START')


if __name__ == '__main__':
    run_tests()
