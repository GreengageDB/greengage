#
# Copyright (c) 2025-Present, Greengage Community
#


from gppylib.test.unit.gp_unittest import *
from mock import *

from ggrebalance_modules.ggrebalance_main_sm import GGRebalanceMainSM
from ggrebalance_modules.rebalance_schema import RebalanceSchema
from ggrebalance_modules.rebalance_step import (
    RebalanceStep,
    RebalanceStepMoveMirror,
    RebalanceStepSwitchoverToMirror,
    RebalanceStepSwitchoverToPrimary,
)

_MOD = 'ggrebalance_modules.ggrebalance_main_sm'

def _make_step(step_cls, status, is_rollback=False, str_repr=None):
    step = MagicMock(spec=step_cls)
    step.getStatus.return_value = status
    step.isRollback.return_value = is_rollback
    if str_repr is None:
        str_repr = f"Rebalance step: {step_cls.__name__}"
    step.__str__.return_value = str_repr
    return step


def _mirror(status, is_rollback=False, str_repr=None):
    return _make_step(RebalanceStepMoveMirror, status, is_rollback, str_repr)


def _switchover1(status, is_rollback=False):
    return _make_step(RebalanceStepSwitchoverToMirror, status, is_rollback)

def _switchover2(status, is_rollback=False):
    return _make_step(RebalanceStepSwitchoverToPrimary, status, is_rollback)


def _make_sm():
    sm = MagicMock()
    sm.logger = Mock(spec=['info', 'warning', 'error', 'debug'])
    sm.summary_separator_str = GGRebalanceMainSM.summary_separator_str
    sm.rebalance_schema = MagicMock()
    sm.rebalance_schema.ProgressType = RebalanceSchema.ProgressType
    return sm


def _make_summary(steps):
    s = MagicMock()
    s.executed_rebalance_steps = steps
    return s


def _make_gp_array(down_flags=None):
    gp_array = MagicMock()
    segs = []
    for i, flag in enumerate(down_flags or []):
        seg = MagicMock()
        seg.isSegmentDown.return_value = flag
        seg.__str__.return_value = f"Segment(content={i}, down={flag})"
        segs.append(seg)
    gp_array.getSegDbList.return_value = segs
    return gp_array


def _call(sm, summary):
    """
    Invoke the real method on a fake SM instance
    """
    GGRebalanceMainSM.print_rebalance_summary(sm, summary)


class TestPrintRebalanceSummary(GpTestCase):
    """
    Unit tests for GGRebalanceMainSM.print_rebalance_summary.

     1.  Empty step list --> early return, zero I/O
     2.  PROGRESS_NO --> skip message logged; REBALANCE table suppressed
     3.  PROGRESS_SIMPLE --> full summary table logged
     4.  PROGRESS_DETAILED --> same as SIMPLE for rebalance section
     5.  segments_moved --> only DONE non-rollback RebalanceStepMoveMirror counted
     6.  rolled_back_steps --> any DONE + isRollback() step
     7.  cancelled_steps --> CANCELLED status
     8.  Unexpected status --> Exception raised before any output
     9.  Mixed statuses --> all three buckets filled simultaneously
    10.  No warnings --> two segments explicitly up, cluster balanced; summary table still printed
    11.  Warning: cancelled steps --> per-step line logged in warnings section
    12.  Warning: segments down --> fault-tolerance message + each segment
    13.  Warning: unbalanced, no rolled-back steps
    14.  Warning: unbalanced WITH rolled-back steps --> retry message printed
    15.  All three warning branches fired in one call
    16.  PROGRESS_NO still fires warnings (warnings are independent of mode)
    17.  Multiple down segments --> each one logged individually and in order
    """

    # 1
    @patch(f'{_MOD}.configurationInterface')
    @patch(f'{_MOD}.is_gparray_balanced', return_value=True)
    def test_empty_steps_returns_early_no_logging(self, _bal, mock_cfg):
        sm = _make_sm()
        _call(sm, _make_summary([]))

        sm.logger.info.assert_not_called()
        sm.logger.warning.assert_not_called()

        mock_cfg.getConfigurationProvider.assert_not_called()

    # 2
    @patch(f'{_MOD}.configurationInterface')
    @patch(f'{_MOD}.is_gparray_balanced', return_value=True)
    def test_progress_no_logs_skip_message_and_suppresses_table(self, _bal, mock_cfg):
        sm = _make_sm()
        sm.rebalance_schema.getProgressType.return_value = (
            RebalanceSchema.ProgressType.PROGRESS_NO)
        mock_cfg.getConfigurationProvider.return_value \
            .loadSystemConfig.return_value = _make_gp_array()

        _call(sm, _make_summary([_mirror(RebalanceStep.Status.DONE)]))

        sm.logger.info.assert_any_call(
            'Skip final rebalance summary report '
            '(specify "--simple-progress" or "--detailed-progress" to enable it).')
        logged = [str(c) for c in sm.logger.info.call_args_list]
        self.assertFalse(
            any('REBALANCE' in m for m in logged),
            "REBALANCE header must not appear in PROGRESS_NO mode")

    # 3
    @patch(f'{_MOD}.configurationInterface')
    @patch(f'{_MOD}.is_gparray_balanced', return_value=True)
    def test_progress_simple_logs_full_table(self, _bal, mock_cfg):
        sm = _make_sm()
        sm.rebalance_schema.getProgressType.return_value = (
            RebalanceSchema.ProgressType.PROGRESS_SIMPLE)
        load_mock = mock_cfg.getConfigurationProvider.return_value.loadSystemConfig
        load_mock.return_value = _make_gp_array()
        steps = [_mirror(RebalanceStep.Status.DONE),
                 _mirror(RebalanceStep.Status.DONE)]
        _call(sm, _make_summary(steps))
        sm.logger.info.assert_any_call(GGRebalanceMainSM.summary_separator_str)
        sm.logger.info.assert_any_call(
            '                                   REBALANCE                                   ')
        sm.logger.info.assert_any_call('Segments moved:\t\t2')
        sm.logger.info.assert_any_call('Rolled back steps:\t\t0')
        sm.logger.info.assert_any_call('Cancelled steps:\t\t0')
        load_mock.assert_called_once_with(useUtilityMode=False, verbose=False)

    # 4
    @patch(f'{_MOD}.configurationInterface')
    @patch(f'{_MOD}.is_gparray_balanced', return_value=True)
    def test_progress_detailed_logs_full_table(self, _bal, mock_cfg):
        sm = _make_sm()
        sm.rebalance_schema.getProgressType.return_value = (
            RebalanceSchema.ProgressType.PROGRESS_DETAILED)
        mock_cfg.getConfigurationProvider.return_value \
            .loadSystemConfig.return_value = _make_gp_array()

        _call(sm, _make_summary([_mirror(RebalanceStep.Status.DONE)]))

        sm.logger.info.assert_any_call('Segments moved:\t\t1')
        sm.logger.info.assert_any_call('Rolled back steps:\t\t0')
        sm.logger.info.assert_any_call('Cancelled steps:\t\t0')
        sm.logger.warning.assert_not_called()

    # 5
    @patch(f'{_MOD}.configurationInterface')
    @patch(f'{_MOD}.is_gparray_balanced', return_value=True)
    def test_segments_moved_excludes_rollback_and_switchovers(self, _bal, mock_cfg):
        sm = _make_sm()
        sm.rebalance_schema.getProgressType.return_value = (
            RebalanceSchema.ProgressType.PROGRESS_SIMPLE)
        mock_cfg.getConfigurationProvider.return_value \
            .loadSystemConfig.return_value = _make_gp_array()
        steps = [
            _mirror(RebalanceStep.Status.DONE, is_rollback=False),  # counted as moved
            _mirror(RebalanceStep.Status.DONE, is_rollback=False),  # counted as moved
            _mirror(RebalanceStep.Status.DONE, is_rollback=True),   # NOT moved; rolled back
            _switchover1(RebalanceStep.Status.DONE, is_rollback=False),  # NOT moved
            _switchover2(RebalanceStep.Status.DONE, is_rollback=False),  # NOT moved
        ]
        _call(sm, _make_summary(steps))
        sm.logger.info.assert_any_call('Segments moved:\t\t2')
        # The rolled-back mirror step must land in its own bucket, not disappear
        sm.logger.info.assert_any_call('Rolled back steps:\t\t1')

    # 6
    @patch(f'{_MOD}.configurationInterface')
    @patch(f'{_MOD}.is_gparray_balanced', return_value=True)
    def test_rolled_back_steps_counted_correctly(self, _bal, mock_cfg):
        sm = _make_sm()
        sm.rebalance_schema.getProgressType.return_value = (
            RebalanceSchema.ProgressType.PROGRESS_SIMPLE)
        mock_cfg.getConfigurationProvider.return_value \
            .loadSystemConfig.return_value = _make_gp_array()

        steps = [_mirror(RebalanceStep.Status.DONE, is_rollback=True),
                 _mirror(RebalanceStep.Status.DONE, is_rollback=True)]
        _call(sm, _make_summary(steps))

        sm.logger.info.assert_any_call('Rolled back steps:\t\t2')
        sm.logger.info.assert_any_call('Segments moved:\t\t0')

    # 7
    @patch(f'{_MOD}.configurationInterface')
    @patch(f'{_MOD}.is_gparray_balanced', return_value=True)
    def test_cancelled_steps_counted_correctly(self, _bal, mock_cfg):
        sm = _make_sm()
        sm.rebalance_schema.getProgressType.return_value = (
            RebalanceSchema.ProgressType.PROGRESS_SIMPLE)
        mock_cfg.getConfigurationProvider.return_value \
            .loadSystemConfig.return_value = _make_gp_array()

        steps = [_mirror(RebalanceStep.Status.CANCELLED) for _ in range(3)]
        _call(sm, _make_summary(steps))

        sm.logger.info.assert_any_call('Cancelled steps:\t\t3')

    # 8
    @patch(f'{_MOD}.configurationInterface')
    @patch(f'{_MOD}.is_gparray_balanced', return_value=True)
    def test_unexpected_status_raises_exception(self, _bal, _cfg):
        sm = _make_sm()
        sm.rebalance_schema.getProgressType.return_value = (
            RebalanceSchema.ProgressType.PROGRESS_SIMPLE)

        bad = MagicMock()
        bad.getStatus.return_value = RebalanceStep.Status.IN_PROGRESS
        bad.__str__.return_value = 'step in progress'

        with self.assertRaises(Exception) as ctx:
            _call(sm, _make_summary([bad]))
        self.assertIn('Unexpected status for executed step', str(ctx.exception))

    # 9
    @patch(f'{_MOD}.configurationInterface')
    @patch(f'{_MOD}.is_gparray_balanced', return_value=True)
    def test_mixed_statuses_all_buckets_filled(self, _bal, mock_cfg):
        sm = _make_sm()
        sm.rebalance_schema.getProgressType.return_value = (
            RebalanceSchema.ProgressType.PROGRESS_SIMPLE)
        mock_cfg.getConfigurationProvider.return_value \
            .loadSystemConfig.return_value = _make_gp_array()

        steps = [
            _mirror(RebalanceStep.Status.DONE, is_rollback=False),  # 1 moved
            _mirror(RebalanceStep.Status.DONE, is_rollback=False),  # 1 moved
            _mirror(RebalanceStep.Status.DONE, is_rollback=True),   # 1 rolled back
            _mirror(RebalanceStep.Status.CANCELLED),                 # 1 cancelled
        ]
        _call(sm, _make_summary(steps))

        sm.logger.info.assert_any_call('Segments moved:\t\t2')
        sm.logger.info.assert_any_call('Rolled back steps:\t\t1')
        sm.logger.info.assert_any_call('Cancelled steps:\t\t1')

    # 10
    @patch(f'{_MOD}.configurationInterface')
    @patch(f'{_MOD}.is_gparray_balanced', return_value=True)
    def test_no_warnings_when_cluster_healthy(self, _bal, mock_cfg):
        sm = _make_sm()
        sm.rebalance_schema.getProgressType.return_value = (
            RebalanceSchema.ProgressType.PROGRESS_SIMPLE)
        # Two segments explicitly UP.
        # This proves that segments being UP suppresses the
        # fault-tolerance warning while the summary table is still emitted.
        mock_cfg.getConfigurationProvider.return_value \
            .loadSystemConfig.return_value = _make_gp_array(down_flags=[False, False])
        _call(sm, _make_summary([_mirror(RebalanceStep.Status.DONE)]))
        sm.logger.info.assert_any_call('Segments moved:\t\t1')
        sm.logger.warning.assert_not_called()

    # 11
    @patch(f'{_MOD}.configurationInterface')
    @patch(f'{_MOD}.is_gparray_balanced', return_value=True)
    def test_warning_for_cancelled_steps(self, _bal, mock_cfg):
        sm = _make_sm()
        sm.rebalance_schema.getProgressType.return_value = (
            RebalanceSchema.ProgressType.PROGRESS_SIMPLE)
        mock_cfg.getConfigurationProvider.return_value \
            .loadSystemConfig.return_value = _make_gp_array()
        c_step = _mirror(
            RebalanceStep.Status.CANCELLED,
            str_repr="Rebalance step, move_order=3 type: mirror move, content=1")
        _call(sm, _make_summary([c_step]))
        expected_line = str(c_step).partition("type: ")[2]
        sm.logger.warning.assert_any_call(expected_line)

    # 12
    @patch(f'{_MOD}.configurationInterface')
    @patch(f'{_MOD}.is_gparray_balanced', return_value=True)
    def test_warning_for_segments_down(self, _bal, mock_cfg):
        sm = _make_sm()
        sm.rebalance_schema.getProgressType.return_value = (
            RebalanceSchema.ProgressType.PROGRESS_SIMPLE)
        down_seg = MagicMock()
        down_seg.isSegmentDown.return_value = True
        down_seg.__str__.return_value = 'sdw2|content=1|role=m|status=d'
        gp_array = MagicMock()
        gp_array.getSegDbList.return_value = [down_seg]
        mock_cfg.getConfigurationProvider.return_value \
            .loadSystemConfig.return_value = gp_array
        _call(sm, _make_summary([_mirror(RebalanceStep.Status.DONE)]))
        sm.logger.warning.assert_any_call(
            'Cluster might be not in fault tolerance mode!')
        sm.logger.warning.assert_any_call(
            'These segments should be started manually in order cluster '
            'to become fault tolerant:')
        sm.logger.warning.assert_any_call(str(down_seg))

    # 13
    @patch(f'{_MOD}.configurationInterface')
    @patch(f'{_MOD}.is_gparray_balanced', return_value=False)
    def test_warning_unbalanced_no_rolled_back_steps(self, _bal, mock_cfg):
        sm = _make_sm()
        sm.rebalance_schema.getProgressType.return_value = (
            RebalanceSchema.ProgressType.PROGRESS_SIMPLE)
        mock_cfg.getConfigurationProvider.return_value \
            .loadSystemConfig.return_value = _make_gp_array()

        _call(sm, _make_summary([_mirror(RebalanceStep.Status.DONE,
                                         is_rollback=False)]))

        sm.logger.warning.assert_any_call('Cluster is left in unbalanced state')
        all_warnings = [str(c) for c in sm.logger.warning.call_args_list]
        self.assertFalse(
            any('Rolled back steps' in w for w in all_warnings),
            "Rolled-back-steps section must not appear when list is empty")
        self.assertFalse(
            any('retry rebalance later' in w for w in all_warnings))

    # 14
    @patch(f'{_MOD}.configurationInterface')
    @patch(f'{_MOD}.is_gparray_balanced', return_value=False)
    def test_warning_unbalanced_with_rolled_back_steps(self, _bal, mock_cfg):
        sm = _make_sm()
        sm.rebalance_schema.getProgressType.return_value = (
            RebalanceSchema.ProgressType.PROGRESS_SIMPLE)
        mock_cfg.getConfigurationProvider.return_value \
            .loadSystemConfig.return_value = _make_gp_array()

        rb = _mirror(
            RebalanceStep.Status.DONE,
            is_rollback=True,
            str_repr="Rebalance step, move_order=2 type: mirror move – rolled back")
        _call(sm, _make_summary([rb]))

        sm.logger.warning.assert_any_call('Cluster is left in unbalanced state')
        sm.logger.warning.assert_any_call(
            '------------------------------ Rolled back steps ---------------------------------')

        expected_line = str(rb).partition("type: ")[2]
        sm.logger.warning.assert_any_call(expected_line)
        sm.logger.warning.assert_any_call(
            'You can review why segments were rolled back and retry rebalance later.')

    # 15
    @patch(f'{_MOD}.configurationInterface')
    @patch(f'{_MOD}.is_gparray_balanced', return_value=False)
    def test_all_warning_branches_fire_together(self, _bal, mock_cfg):
        sm = _make_sm()
        sm.rebalance_schema.getProgressType.return_value = (
            RebalanceSchema.ProgressType.PROGRESS_DETAILED)
        down_seg = MagicMock()
        down_seg.isSegmentDown.return_value = True
        down_seg.__str__.return_value = 'Segment(down=True)'
        gp_array = MagicMock()
        gp_array.getSegDbList.return_value = [down_seg]
        mock_cfg.getConfigurationProvider.return_value \
            .loadSystemConfig.return_value = gp_array
        rb = _mirror(RebalanceStep.Status.DONE, is_rollback=True,
                     str_repr="info type: rb detail")
        c = _mirror(RebalanceStep.Status.CANCELLED,
                    str_repr="info type: cancelled detail")
        _call(sm, _make_summary([rb, c]))
        # Structural headers of the WARNINGS block — asserted only here
        sm.logger.warning.assert_any_call(GGRebalanceMainSM.summary_separator_str)
        sm.logger.warning.assert_any_call('                                   WARNINGS                                    ')
        # Branch 1: fault tolerance (down segment)
        sm.logger.warning.assert_any_call('Cluster might be not in fault tolerance mode!')
        # Branch 2: cancelled steps sub-header — asserted only here, not in #11
        sm.logger.warning.assert_any_call(
            '------------------------------- Cancelled steps  -------------------------------')
        # Branch 3: unbalanced cluster + rolled-back steps sub-header
        sm.logger.warning.assert_any_call('Cluster is left in unbalanced state')
        sm.logger.warning.assert_any_call(
            '------------------------------ Rolled back steps ---------------------------------')
        sm.logger.warning.assert_any_call(
            'You can review why segments were rolled back and retry rebalance later.')

    # 16
    @patch(f'{_MOD}.configurationInterface')
    @patch(f'{_MOD}.is_gparray_balanced', return_value=False)
    def test_progress_no_still_fires_warnings(self, _bal, mock_cfg):
        sm = _make_sm()
        sm.rebalance_schema.getProgressType.return_value = (
            RebalanceSchema.ProgressType.PROGRESS_NO)
        # Down segment triggers the fault-tolerance branch
        down_seg = MagicMock()
        down_seg.isSegmentDown.return_value = True
        down_seg.__str__.return_value = 'Segment(down=True)'
        gp_array = MagicMock()
        gp_array.getSegDbList.return_value = [down_seg]
        mock_cfg.getConfigurationProvider.return_value \
            .loadSystemConfig.return_value = gp_array
        # Cancelled step triggers the cancelled-steps branch
        c_step = _mirror(
            RebalanceStep.Status.CANCELLED,
            str_repr="Rebalance step, move_order=1 type: mirror move, content=0")
        _call(sm, _make_summary([c_step]))
        # Info table suppressed
        sm.logger.info.assert_any_call(
            'Skip final rebalance summary report '
            '(specify "--simple-progress" or "--detailed-progress" to enable it).')
        # All three warning branches still fire despite PROGRESS_NO
        sm.logger.warning.assert_any_call('Cluster is left in unbalanced state')
        sm.logger.warning.assert_any_call('Cluster might be not in fault tolerance mode!')
        sm.logger.warning.assert_any_call(str(down_seg))
        expected_cancelled_line = str(c_step).partition("type: ")[2]
        sm.logger.warning.assert_any_call(expected_cancelled_line)

    # 17
    @patch(f'{_MOD}.configurationInterface')
    @patch(f'{_MOD}.is_gparray_balanced', return_value=True)
    def test_multiple_down_segments_each_logged(self, _bal, mock_cfg):
        sm = _make_sm()
        sm.rebalance_schema.getProgressType.return_value = (
            RebalanceSchema.ProgressType.PROGRESS_SIMPLE)
        down_segs = []
        for i in range(3):
            seg = MagicMock()
            seg.isSegmentDown.return_value = True
            seg.__str__.return_value = f"Segment(content={i})"
            down_segs.append(seg)
        gp_array = MagicMock()
        gp_array.getSegDbList.return_value = down_segs
        mock_cfg.getConfigurationProvider.return_value \
            .loadSystemConfig.return_value = gp_array
        _call(sm, _make_summary([_mirror(RebalanceStep.Status.DONE)]))
        sm.logger.warning.assert_has_calls(
            [call(str(s)) for s in down_segs], any_order=False)


if __name__ == '__main__':
    run_tests()
