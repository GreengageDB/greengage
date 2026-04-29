#!/usr/bin/env python3
#
# Copyright (c) 2025-Present, Greengage Community
#

from transitions import Machine
from contextlib import closing
from typing import Any

try:
    from gppylib.commands.unix import *
    from gppylib.commands.gp import *
    from gppylib.gplog import *
    from gppylib.db import dbconn
    from gppylib.gparray import GpArray, Segment
    from gppylib.fault_injection import *
    from gppylib.userinput import *
    from gppylib.commands import base
    from gppylib.commands.gp import SEGMENT_STOP_TIMEOUT_DEFAULT, SegmentStop
    from gppylib.system.environment import *
    from gppylib.utils import escape_string, escapeDoubleQuoteInSQLString
    from ggrebalance_modules.planner import *
    from ggrebalance_modules.rebalance_commons import interactive_check_yesno
    from ggrebalance_modules.rebalance_schema import RebalanceSchema, STATE_NOT_DEFINED, get_table_distr_segment_count
except ImportError as e:
    sys.exit('ERROR: Cannot import modules.  Please check that you have sourced greenplum_path.sh.  Detail: ' + str(e))

class GGShrink:
    timeout = SEGMENT_STOP_TIMEOUT_DEFAULT

    states = [
        'STATE_START',
        'STATE_CHECK_PREVIOUS_RUN',
        'STATE_END',
        'STATE_ERROR',
        'STATE_END_FROM_ROLLBACK'
    ]

    # Note: order of states in the list below is important,
    # as we rely on it when recover from an interrupted state.
    # These states are separated from 'states' above, because
    # these states exist after the shrink schema is created,
    # so they are reflected in the status table inside the schema,
    # and we can re-enter the interrupted state.
    states_main_shrink_flow = [
        'STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_STARTED',
        'STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_DONE',
        'STATE_PREPARE_SHRINK_SCHEMA_STARTED',
        'STATE_PREPARE_SHRINK_SCHEMA_DONE',
        'STATE_SHRINK_TABLES_STARTED',
        'STATE_SHRINK_TABLES_DONE',
        'STATE_SHRINK_CATALOG_STARTED',
        'STATE_SHRINK_CATALOG_DONE',
        'STATE_SHRINK_SEGMENTS_STOP_STARTED',
        'STATE_SHRINK_SEGMENTS_STOP_DONE',
        'STATE_SHRINK_DONE'
    ]

    # Note: order of states in the list below is important,
    # as we rely on it when recover from an interrupted state.
    # These states are separated from 'states' and 'states_main_shrink_flow'
    # above in order to support re-enter the interrupted state of rollback.
    states_rollback_flow = [
        'STATE_SHRINK_ROLLBACK_RESTORE_TARGET_SEGMENT_COUNT_START',
        'STATE_SHRINK_ROLLBACK_RESTORE_TARGET_SEGMENT_COUNT_DONE',
        'STATE_SHRINK_ROLLBACK_PREPARE_SCHEMA_START',
        'STATE_SHRINK_ROLLBACK_PREPARE_SCHEMA_DONE',
        'STATE_SHRINK_ROLLBACK_SHRINKED_TABLES_START',
        'STATE_SHRINK_ROLLBACK_SHRINKED_TABLES_DONE',
        'STATE_SHRINK_ROLLBACK_DROP_SCHEMA_START',
        'STATE_SHRINK_ROLLBACK_DROP_SCHEMA_DONE'
    ]

    transitions = [
        {
            'trigger': 'start',
            'source': 'STATE_START',
            'dest': 'STATE_CHECK_PREVIOUS_RUN'
        },
        {
            'trigger': 'move_to_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_STARTED',
            'source': 'STATE_CHECK_PREVIOUS_RUN',
            'dest':  'STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_STARTED'
        },
        {
            'trigger': 'move_to_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_DONE',
            'source': 'STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_STARTED',
            'dest': 'STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_DONE'
        },
        {
            'trigger': 'move_to_STATE_PREPARE_SHRINK_SCHEMA_STARTED',
            'source': 'STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_DONE',
            'dest': 'STATE_PREPARE_SHRINK_SCHEMA_STARTED'
        },
        {
            'trigger': 'move_to_STATE_PREPARE_SHRINK_SCHEMA_DONE',
            'source': 'STATE_PREPARE_SHRINK_SCHEMA_STARTED',
            'dest': 'STATE_PREPARE_SHRINK_SCHEMA_DONE'
        },
        {
            'trigger': 'move_to_STATE_SHRINK_TABLES_STARTED',
            'source': 'STATE_PREPARE_SHRINK_SCHEMA_DONE',
            'dest': 'STATE_SHRINK_TABLES_STARTED'
        },
        {
            'trigger': 'move_to_STATE_SHRINK_TABLES_DONE',
            'source': 'STATE_SHRINK_TABLES_STARTED',
            'dest': 'STATE_SHRINK_TABLES_DONE'
        },
        {
            'trigger': 'move_to_STATE_SHRINK_CATALOG_STARTED',
            'source': 'STATE_SHRINK_TABLES_DONE',
            'dest': 'STATE_SHRINK_CATALOG_STARTED'
        },
        {
            'trigger': 'move_to_STATE_SHRINK_CATALOG_DONE',
            'source': 'STATE_SHRINK_CATALOG_STARTED',
            'dest': 'STATE_SHRINK_CATALOG_DONE'
        },
        {
            'trigger': 'move_to_STATE_SHRINK_SEGMENTS_STOP_STARTED',
            'source': 'STATE_SHRINK_CATALOG_DONE',
            'dest': 'STATE_SHRINK_SEGMENTS_STOP_STARTED'
        },
        {
            'trigger': 'move_to_STATE_SHRINK_SEGMENTS_STOP_DONE',
            'source': 'STATE_SHRINK_SEGMENTS_STOP_STARTED',
            'dest': 'STATE_SHRINK_SEGMENTS_STOP_DONE'
        },
        {
            'trigger': 'move_to_STATE_SHRINK_DONE',
            'source': 'STATE_SHRINK_SEGMENTS_STOP_DONE',
            'dest': 'STATE_SHRINK_DONE'
        },
        {
            'trigger': 'move_to_STATE_END',
            'source': ['STATE_SHRINK_DONE', 'STATE_CHECK_PREVIOUS_RUN', 'STATE_END_FROM_ROLLBACK'],
            'dest': 'STATE_END'
        },
        {
            'trigger': 'move_to_STATE_SHRINK_ROLLBACK_RESTORE_TARGET_SEGMENT_COUNT_START',
            'source': 'STATE_CHECK_PREVIOUS_RUN',
            'dest': 'STATE_SHRINK_ROLLBACK_RESTORE_TARGET_SEGMENT_COUNT_START'
        },
        {
            'trigger': 'move_to_STATE_SHRINK_ROLLBACK_RESTORE_TARGET_SEGMENT_COUNT_DONE',
            'source': 'STATE_SHRINK_ROLLBACK_RESTORE_TARGET_SEGMENT_COUNT_START',
            'dest': 'STATE_SHRINK_ROLLBACK_RESTORE_TARGET_SEGMENT_COUNT_DONE'
        },
        {
            'trigger': 'move_to_STATE_SHRINK_ROLLBACK_PREPARE_SCHEMA_START',
            'source': 'STATE_SHRINK_ROLLBACK_RESTORE_TARGET_SEGMENT_COUNT_DONE',
            'dest': 'STATE_SHRINK_ROLLBACK_PREPARE_SCHEMA_START'
        },
        {
            'trigger': 'move_to_STATE_SHRINK_ROLLBACK_PREPARE_SCHEMA_DONE',
            'source': 'STATE_SHRINK_ROLLBACK_PREPARE_SCHEMA_START',
            'dest': 'STATE_SHRINK_ROLLBACK_PREPARE_SCHEMA_DONE'
        },
        {
            'trigger': 'move_to_STATE_SHRINK_ROLLBACK_SHRINKED_TABLES_START',
            'source': 'STATE_SHRINK_ROLLBACK_PREPARE_SCHEMA_DONE',
            'dest': 'STATE_SHRINK_ROLLBACK_SHRINKED_TABLES_START'
        },
        {
            'trigger': 'move_to_STATE_SHRINK_ROLLBACK_SHRINKED_TABLES_DONE',
            'source': 'STATE_SHRINK_ROLLBACK_SHRINKED_TABLES_START',
            'dest': 'STATE_SHRINK_ROLLBACK_SHRINKED_TABLES_DONE'
        },
        {
            'trigger': 'move_to_STATE_SHRINK_ROLLBACK_DROP_SCHEMA_START',
            'source': 'STATE_SHRINK_ROLLBACK_SHRINKED_TABLES_DONE',
            'dest': 'STATE_SHRINK_ROLLBACK_DROP_SCHEMA_START'
        },
        {
            'trigger': 'move_to_STATE_SHRINK_ROLLBACK_DROP_SCHEMA_DONE',
            'source': 'STATE_SHRINK_ROLLBACK_DROP_SCHEMA_START',
            'dest': 'STATE_SHRINK_ROLLBACK_DROP_SCHEMA_DONE'
        },
        {
            'trigger': 'move_to_STATE_END_FROM_ROLLBACK',
            'source': ['STATE_SHRINK_ROLLBACK_DROP_SCHEMA_DONE', 'STATE_CHECK_PREVIOUS_RUN', 'STATE_START'],
            'dest': 'STATE_END_FROM_ROLLBACK'
        },
        {
            'trigger': 'move_to_STATE_ERROR',
            'source': '*',
            'dest': 'STATE_ERROR'
        }
    ]

    def __init__(self, conn: dbconn.Connection,
                 schema: RebalanceSchema, logger: Any, options: Any, gpEnv: GpCoordinatorEnvironment, gpArray: gparray.GpArray, gpArrayDumpFilename: str) -> None:
        self.logger = logger
        self.options = options
        self.gpEnv = gpEnv
        self.conn = conn
        self.shutdown_requested = False
        self.workers_for_tables_rebalance = None
        self.tables_rebalance_failed = False
        self.workers_for_segment_stop = None
        self.gparray = gpArray
        self.gparray_dump_file = gpArrayDumpFilename
        self.rebalance_schema = schema
        self.shrink_plan = None
        self.dumped_gparray = gparray.GpArray.initFromFile(self.gparray_dump_file) if os.path.exists(self.gparray_dump_file) else None

        if self.options.interactive:
            self.stop_mode = 'smart'
        else:
            self.stop_mode = 'fast'

        self.machine = Machine(model = self,
                               queued=True,
                               states = self.states + self.states_main_shrink_flow + self.states_rollback_flow,
                               transitions = self.transitions,
                               initial = 'STATE_START',
                               before_state_change = 'on_every_state')

    def run(self, shrinkPlan: ShrinkPlan) -> None:
        self.shrink_plan = shrinkPlan
        self.trigger('start')

    def rollback(self, shrinkPlan: ShrinkPlan) -> None:
        self.shrink_plan = shrinkPlan
        if not self.rebalance_schema.schemaExists():
            self.logger.info("Rebalance schema doesn't exist. Can't perform rollback.")
            self.trigger('move_to_STATE_END_FROM_ROLLBACK')
            return
        else:
            state_from_prev_run = self.rebalance_schema.getShrinkStateFromPreviousRun()
            if state_from_prev_run != STATE_NOT_DEFINED:
                # check maybe the state is the final one
                if self.state_is_final(state_from_prev_run):
                    self.logger.info("Previous run was completed successfully. Can't perform rollback.")
                    self.trigger('move_to_STATE_END_FROM_ROLLBACK')
                    return

                if not self.state_can_rollback(state_from_prev_run) or self.is_gp_segment_configuration_shrinked():
                    self.logger.info("Can't perform rollback as the catalog is already updated")
                    self.trigger('move_to_STATE_END_FROM_ROLLBACK')
                    return

        self.trigger('start')

    def get_state_after_interrupt(self, prev_state) -> str:
        prev_idx = self.states_main_shrink_flow.index(prev_state)
        lower = self.states_main_shrink_flow.index('STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_STARTED')
        upper = self.states_main_shrink_flow.index('STATE_SHRINK_TABLES_DONE')
        if prev_idx >= lower and prev_idx <= upper:

            #if shrink is interrupted after catalog update and before the state is logged
            if prev_state == 'STATE_SHRINK_TABLES_DONE' and \
                self.dumped_gparray is not None \
                and self.gparray.get_segment_count() + self.shrink_plan.target_segment_count == self.dumped_gparray.get_segment_count():
                return 'STATE_SHRINK_CATALOG_DONE'

            row = dbconn.queryRow(self.conn, 'SELECT gp_toolkit.gp_rebalance_numsegments_is_set();')
            # means that target rebalance numsegments is reset, and new tables are created at old segment count
            if bool(row[0]) is False:
                self.logger.info("Cluster restarted after previous run, trying to repopulate the relation queue")
                return 'STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_STARTED'
        
        return self.states_main_shrink_flow[prev_idx + 1]

    def on_every_state(self) -> None:
        if self.shutdown_requested:
            self.logger.info('Shrink was interrupted')
            raise Exception('Shrink was interrupted')

        assert self.state in self.states + self.states_main_shrink_flow + self.states_rollback_flow

        if self.state in self.states_main_shrink_flow + self.states_rollback_flow:
            self.rebalance_schema.storeShrinkState(self.state)

    def cleanup(self, prev_run_was_complete: bool) -> bool:
        if not prev_run_was_complete:
            self.logger.warning("ggrebalance hasn't finished shrink process properly. Previous run was interrupted. "
                                "Some unbalanced tables can still exist.")

            # get default num segments
            dbconn.execSQL(self.conn, 'BEGIN')
            dbconn.execSQL(self.conn, 'SELECT gp_expand_lock_catalog()')
            row = dbconn.queryRow(self.conn, 'SELECT gp_toolkit.gp_rebalance_numsegments_is_set()')
            numsegments_is_set = bool(row[0])
            dbconn.execSQL(self.conn, 'END')

            if numsegments_is_set:
                self.logger.warning('Current numsegments is not equal to default value.')
                self.logger.info('Suggestion: explicitly reset the value before cleanup. Note: cluster restart will implicitly reset the value.')

            if not interactive_check_yesno(self.options.interactive, None, '\nContinue with cleanup?', default = 'Y'):
                self.logger.info('Cleanup was interrupted...')
                return False

            if (numsegments_is_set and
                interactive_check_yesno(self.options.interactive, None, '\nReset numsegments to default?', default = 'Y')):
                dbconn.execSQL(self.conn, 'BEGIN')
                dbconn.execSQL(self.conn, 'SELECT gp_expand_lock_catalog()')
                dbconn.execSQL(self.conn, 'SELECT gp_toolkit.gp_reset_rebalance_numsegments()')
                dbconn.execSQL(self.conn, 'COMMIT')
                self.logger.info('Reset numsegments to default is done.')

        if os.path.exists(self.gparray_dump_file):
            os.remove(self.gparray_dump_file)

        return True

    def state_is_final(self, state: str) -> bool:
        return state == self.states_main_shrink_flow[-1]

    def state_is_from_rollback_flow(self, state: str) -> bool:
        return state in self.states_rollback_flow

    # state callbacks start here

    @wrap_func_with_faults
    def on_enter_STATE_CHECK_PREVIOUS_RUN(self) -> None:
        assert self.rebalance_schema.schemaExists()
        # check whether we can get the state where we stopped in previous run
        # in order to proceed from the same point
        state_from_prev_run = self.rebalance_schema.getShrinkStateFromPreviousRun()
        # check maybe the state is the final one
        if self.state_is_final(state_from_prev_run):
            if self.options.rollback_required:
                self.logger.info(f"Previous run was completed successfully. Can't perform rollback.")
                self.trigger('move_to_STATE_END_FROM_ROLLBACK')
        else:
            if self.state_is_from_rollback_flow(state_from_prev_run):
                self.logger.info('Continue interrupted shrink rollback operation...')
                self.logger.info(f"Previous run stopped after state '{state_from_prev_run}', trying to continue from the next state...")
                try:
                    next_state = self.states_rollback_flow[ self.states_rollback_flow.index(state_from_prev_run) + 1 ]
                except:
                    self.logger.error("Can't determine next rollback state")
                    self.trigger('move_to_STATE_ERROR')
                    return
            else:
                if self.options.rollback_required:
                    self.trigger('move_to_STATE_SHRINK_ROLLBACK_RESTORE_TARGET_SEGMENT_COUNT_START')
                    return

                # no state so far, so start from the beginning
                if state_from_prev_run == STATE_NOT_DEFINED:
                    self.trigger('move_to_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_STARTED')
                    return

                self.logger.info('Continue interrupted shrink operation...')
                self.logger.info(f"Previous run stopped after state '{state_from_prev_run}', trying to continue from the next state...")
                try:
                    next_state = self.get_state_after_interrupt(state_from_prev_run)
                except:
                    self.logger.error("Can't determine next state. Try to execute cleanup.")
                    self.trigger('move_to_STATE_ERROR')
                    return

            if not interactive_check_yesno(self.options.interactive, None, 'Proceed with continue?', default = 'Y'):
                raise Exception('Continue was not approved, interrupting execution')
            # use auto to_«state» method to recover
            self.trigger(f'to_{next_state}')

    @wrap_func_with_faults
    def on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_STARTED(self) -> None:
        dbconn.execSQL(self.conn, 'BEGIN')
        dbconn.execSQL(self.conn, 'SELECT gp_expand_lock_catalog()')
        dbconn.execSQL(self.conn, 'CHECKPOINT')
        dbconn.execSQL(self.conn, f'SELECT gp_toolkit.gp_set_rebalance_numsegments({self.shrink_plan.getTargetSegmentCount()})')

        self.gparray.dumpToFile(self.gparray_dump_file)

        # Rebalance the status tables we've created previously right here before we start to rebalance all other tables.
        self.rebalance_schema.rebalanceSchema(self.shrink_plan.getTargetSegmentCount())

        dbconn.execSQL(self.conn, 'COMMIT')

        self.trigger('move_to_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_DONE')

    @wrap_func_with_faults
    def on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_DONE(self) -> None:
        self.logger.info(f'Updated target segment count to {self.shrink_plan.getTargetSegmentCount()}')
        self.trigger('move_to_STATE_PREPARE_SHRINK_SCHEMA_STARTED')

    @wrap_func_with_faults
    def on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED(self) -> None:
        self.prepare_shrink_schema(False)
        self.trigger('move_to_STATE_PREPARE_SHRINK_SCHEMA_DONE')

    @wrap_func_with_faults
    def on_enter_STATE_PREPARE_SHRINK_SCHEMA_DONE(self) -> None:
        self.logger.info(f'Initiated list of tables to rebalance')
        self.trigger('move_to_STATE_SHRINK_TABLES_STARTED')

    @wrap_func_with_faults
    def on_enter_STATE_SHRINK_TABLES_STARTED(self) -> None:
        self.logger.info('Start tables rebalance for shrink')
        # perform 'ALTER TABLE REBALANCE' for all not yet processed tables
        self.rebalance_tables('none', 'done', self.shrink_plan.getTargetSegmentCount())
        self.trigger('move_to_STATE_SHRINK_TABLES_DONE')

    @wrap_func_with_faults
    def on_enter_STATE_SHRINK_TABLES_DONE(self) -> None:
        self.logger.info('Tables rebalance complete')
        self.trigger('move_to_STATE_SHRINK_CATALOG_STARTED')

    @wrap_func_with_faults
    def on_enter_STATE_SHRINK_CATALOG_STARTED(self) -> None:
        self.logger.info('Start catalog shrink')

        ## Shrink catalog
        dbconn.execSQL(self.conn, 'BEGIN')
        dbconn.execSQL(self.conn, 'SELECT gp_expand_lock_catalog()')
        dbconn.execSQL(self.conn, f'DELETE FROM gp_segment_configuration WHERE content >= {self.shrink_plan.getTargetSegmentCount()}')
        dbconn.execSQL(self.conn, 'CHECKPOINT')
        dbconn.execSQL(self.conn, 'SELECT gp_expand_bump_version()')
        dbconn.execSQL(self.conn, 'SELECT gp_toolkit.gp_reset_rebalance_numsegments()')
        dbconn.execSQL(self.conn, 'COMMIT')

        self.trigger('move_to_STATE_SHRINK_CATALOG_DONE')

    @wrap_func_with_faults
    def on_enter_STATE_SHRINK_CATALOG_DONE(self) -> None:
        self.logger.info('Catalog shrink complete')
        self.trigger('move_to_STATE_SHRINK_SEGMENTS_STOP_STARTED')

    @wrap_func_with_faults
    def on_enter_STATE_SHRINK_SEGMENTS_STOP_STARTED(self) -> None:
        self.logger.info('Stopping shrinked segments...')

        gp_array = self.dumped_gparray

        if gp_array is None:
            gp_array = self.gparray
        
        segments_to_stop = gp_array.get_segment_count() - self.shrink_plan.getTargetSegmentCount()
        self.workers_for_segment_stop = WorkerPool(numWorkers=min(segments_to_stop, self.options.batch_size))

        segments_stop_successful = []
        segments_stop_failed = []
        segments_stop_skipped = []

        # Stop primaries first, and mirrors after primaries,
        # to avoid hanging replication processes
        seg_roles = [gparray.ROLE_PRIMARY, gparray.ROLE_MIRROR]
        for seg_role in seg_roles:
            self.logger.debug(f"Prepare to stop (mode={self.stop_mode}) segments with role '{seg_role}'")
            for seg in gp_array.getSegDbList():
                if (seg.getSegmentContentId() >= self.shrink_plan.getTargetSegmentCount() and
                    seg.getSegmentRole() == seg_role and seg.isSegmentUp()):
                    if (seg_role == gparray.ROLE_MIRROR and
                        any(seg.getSegmentContentId() == failed_seg.getSegmentContentId() for failed_seg in segments_stop_failed)):
                        segments_stop_skipped.append(seg)
                        continue
                    cmd = self.SegmentStopAfterShrink(self, seg)
                    self.workers_for_segment_stop.addCommand(cmd)
            if self.shutdown_requested:
                break
            self.workers_for_segment_stop.join()
            for task in self.workers_for_segment_stop.getCompletedItems():
                if task.was_successful():
                    segments_stop_successful.append(task.segment)
                else:
                    segments_stop_failed.append(task.segment)

        self.workers_for_segment_stop.haltWork()
        self.workers_for_segment_stop.joinWorkers()

        self.logger.info('Summary of shrinked segments:')
        for seg in segments_stop_successful:
            self.logger.info(f'segment stopped ok - {str(seg)}')
        for seg in segments_stop_failed:
            self.logger.info(f'segment failed to stop - {str(seg)}')
        for seg in segments_stop_skipped:
            self.logger.info(f'segment stop skipped - {str(seg)}')

        self.workers_for_segment_stop = None

        self.trigger('move_to_STATE_SHRINK_SEGMENTS_STOP_DONE')

    @wrap_func_with_faults
    def on_enter_STATE_SHRINK_SEGMENTS_STOP_DONE(self) -> None:
        self.trigger('move_to_STATE_SHRINK_DONE')

    @wrap_func_with_faults
    def on_enter_STATE_SHRINK_DONE(self) -> None:
        os.remove(self.gparray_dump_file)
        self.logger.info('Shrink is complete')
        self.trigger('move_to_STATE_END')

    @wrap_func_with_faults
    def on_enter_STATE_SHRINK_ROLLBACK_RESTORE_TARGET_SEGMENT_COUNT_START(self) -> None:
        self.rebalance_schema.backupShrinkProgress()
        dbconn.execSQL(self.conn, 'BEGIN')
        dbconn.execSQL(self.conn, 'SELECT gp_expand_lock_catalog()')
        dbconn.execSQL(self.conn, 'SELECT gp_toolkit.gp_reset_rebalance_numsegments()')
        # Store state here in case we fail before we enter 'on_every_state()'
        # because after COMMIT we are on a one-way road of rollback.
        self.rebalance_schema.storeShrinkState(self.state)
        dbconn.execSQL(self.conn, 'COMMIT')

        self.trigger('move_to_STATE_SHRINK_ROLLBACK_RESTORE_TARGET_SEGMENT_COUNT_DONE')

    @wrap_func_with_faults
    def on_enter_STATE_SHRINK_ROLLBACK_RESTORE_TARGET_SEGMENT_COUNT_DONE(self) -> None:
        # In some rare cases there can be tables left, that
        # have been already redistributed, but the status is still 'none'.
        # To handle it, we cleanup all tables with status 'none' at the first
        # enter of rollback operation.
        self.rebalance_schema.clearTablesToRebalanceWithStatus('none')

        self.trigger('move_to_STATE_SHRINK_ROLLBACK_PREPARE_SCHEMA_START')

    @wrap_func_with_faults
    def on_enter_STATE_SHRINK_ROLLBACK_PREPARE_SCHEMA_START(self) -> None:
        self.prepare_shrink_schema(True)
        self.trigger('move_to_STATE_SHRINK_ROLLBACK_PREPARE_SCHEMA_DONE')

    @wrap_func_with_faults
    def on_enter_STATE_SHRINK_ROLLBACK_PREPARE_SCHEMA_DONE(self) -> None:
        self.trigger('move_to_STATE_SHRINK_ROLLBACK_SHRINKED_TABLES_START')

    @wrap_func_with_faults
    def on_enter_STATE_SHRINK_ROLLBACK_SHRINKED_TABLES_START(self) -> None:
        self.logger.info('Start tables rebalance for rollback')
        # perform 'ALTER TABLE REBALANCE' for all not yet processed tables
        self.rebalance_tables('done', 'none', self.gparray.get_segment_count())
        self.trigger('move_to_STATE_SHRINK_ROLLBACK_SHRINKED_TABLES_DONE')

    @wrap_func_with_faults
    def on_enter_STATE_SHRINK_ROLLBACK_SHRINKED_TABLES_DONE(self) -> None:
        self.trigger('move_to_STATE_SHRINK_ROLLBACK_DROP_SCHEMA_START')

    @wrap_func_with_faults
    def on_enter_STATE_SHRINK_ROLLBACK_DROP_SCHEMA_START(self) -> None:
        if os.path.exists(self.gparray_dump_file):
            os.remove(self.gparray_dump_file)
        self.rebalance_schema.dropSchema()
        self.trigger('move_to_STATE_SHRINK_ROLLBACK_DROP_SCHEMA_DONE')

    @wrap_func_with_faults
    def on_enter_STATE_SHRINK_ROLLBACK_DROP_SCHEMA_DONE(self) -> None:
        self.logger.info('Rollback is complete.')
        self.trigger('move_to_STATE_END_FROM_ROLLBACK')

    @wrap_func_with_faults
    def on_enter_STATE_END_FROM_ROLLBACK(self) -> None:
        self.trigger('move_to_STATE_END')

    @wrap_func_with_faults
    def on_enter_STATE_END(self) -> None:
        pass

    @wrap_func_with_faults
    def on_enter_STATE_ERROR(self) -> None:
        raise Exception('Shrink entered STATE_ERROR')

    # state callbacks end here

    class SegmentStopAfterShrink(SegmentStop):
        def __init__(self, shrink: 'GGShrink', segment: Segment) -> None:
            self.shrink = shrink
            self.segment = segment
            if self.segment.isSegmentPrimary():
                name = f'stop primary (content {self.segment.getSegmentContentId()}, dbid {self.segment.getSegmentDbId()})'
            else:
                name = f'stop mirror (content {self.segment.getSegmentContentId()}, dbid {self.segment.getSegmentDbId()})'
            self.checkRunningSegment = SegmentIsShutDown(name, self.segment.getSegmentDataDirectory(), base.REMOTE, self.segment.getSegmentHostName())
            SegmentStop.__init__(self,
                                 name,
                                 self.segment.getSegmentDataDirectory(),
                                 self.shrink.stop_mode,
                                 False,
                                 base.REMOTE,
                                 self.segment.getSegmentHostName(),
                                 self.shrink.timeout)

        # decorator to inject a fault before running SegmentStopAfterShrink for a specific dbid
        def wrap_segment_stop_with_faults(fun):
            def func_with_faults(self):
                try:
                    inject_fault(f'fault_segment_stop_dbid_{self.segment.getSegmentDbId()}')
                except Exception as e:
                    os.kill(os.getpid(), signal.SIGINT)
                    return
                fun(self)
            return func_with_faults

        @wrap_segment_stop_with_faults
        def run(self) -> None:
            self.shrink.logger.debug(f'Stopping shrinked segment {str(self.segment)}')
            self.checkRunningSegment.run()
            if self.checkRunningSegment.is_shutdown():
                self.shrink.logger.debug(f'Segment {str(self.segment)} is already down')
                self.set_results(CommandResult(0, b'', b'', True, False))
            else:
                try:
                    SegmentStop.run(self, validateAfter = True)
                except ExecutionError:
                    self.shrink.logger.debug(f'Failed to stop shrinked segment {str(self.segment)}')
                    return
                self.shrink.logger.debug(f'Stopped shrinked segment {str(self.segment)}')

    class TableRebalanceTask(SQLCommand):
        def __init__(self,
                     shrink: 'GGShrink',
                     db_name: str,
                     schema_name: str,
                     rel_name: str,
                     target_segment_count: int,
                     table_status_after_rebalance: str) -> None:
            self.shrink = shrink
            self.db_name = db_name
            self.schema_name = schema_name
            self.rel_name = rel_name
            self.target_segment_count = target_segment_count
            self.table_status_after_rebalance = table_status_after_rebalance
            self.long_operation_in_progress = False
            SQLCommand.__init__(self, f'task rebalance for {self.db_name}.{self.schema_name}.{self.rel_name}')

        # decorator to inject a fault before running TableRebalanceTask for a specific {db_name, schema_name, rel_name}
        def wrap_table_rebalance_with_faults(fun):
            def func_with_faults(self, attempt: int):
                inject_fault(f'fault_rebalance_table_{self.db_name}.{self.schema_name}.{self.rel_name}')
                fun(self, attempt)
            return func_with_faults

        # decorator to help with interruption of potentially long queries
        def long_object_method(func):
            def long_func(*args, **kwargs):
                try:
                    self = args[0]
                    self.long_operation_in_progress = True
                    self.check_cancel()
                    return func(*args, **kwargs)
                finally:
                    self.long_operation_in_progress = False
            return long_func

        def table_exists(self, conn: dbconn.Connection) -> bool:
            if dbconn.querySingleton(conn, f"""
                SELECT count(1)
                FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
                WHERE c.relname = '{escape_string(self.rel_name)}' AND n.nspname = '{escape_string(self.schema_name)}'
                """) == 0:
                return False
            return True

        def db_exists(self, conn: dbconn.Connection) -> bool:
            if dbconn.querySingleton(conn, f"""SELECT count(*) FROM pg_database WHERE datname = '{escape_string(self.db_name)}'""") == 0:
                return False
            return True

        def table_is_rebalanced(self, conn: dbconn.Connection) -> bool:
            if get_table_distr_segment_count(conn, self.schema_name, self.rel_name) != self.target_segment_count:
                return False
            return True

        @long_object_method
        def rebalance_table(self, conn: dbconn.Connection) -> None:
            dbconn.execSQL(conn,
                           f'''ALTER TABLE {escapeDoubleQuoteInSQLString(self.schema_name, True)}.{escapeDoubleQuoteInSQLString(self.rel_name, True)}
                           REBALANCE {self.target_segment_count}''')

        @long_object_method
        def analyze_table(self, conn: dbconn.Connection) -> None:
            dbconn.execSQL(conn,
                           f'''ANALYZE {escapeDoubleQuoteInSQLString(self.schema_name, True)}.{escapeDoubleQuoteInSQLString(self.rel_name, True)}''')

        def check_cancel(self):
            if self.cancel_flag:
                raise Exception(f'Cancelled table rebalance for "{self.db_name}"."{self.schema_name}"."{self.rel_name}"')

        @wrap_table_rebalance_with_faults
        def process_table(self, attempt: int) -> None:
            self.shrink.logger.info(f'Start table rebalance for "{self.db_name}"."{self.schema_name}"."{self.rel_name}" to {self.target_segment_count} segments (attempt {attempt})')
            # check for cancel_flag at the beginning and before each long operation (refer to long_object_method decorator)
            self.check_cancel()
            self.shrink.rebalance_schema.setTableRebalanceStartTime(self.db_name, self.schema_name, self.rel_name)
            if self.db_exists(self.shrink.rebalance_schema.conn):
                dburl = dbconn.DbURL(dbname=self.db_name, port=self.shrink.gpEnv.getCoordinatorPort())
                with closing(dbconn.connect(dburl, encoding='UTF8')) as conn:
                    self.cancel_conn = conn

                    dbconn.execSQL(conn, 'BEGIN')

                    if self.table_exists(conn):
                        if not self.table_is_rebalanced(conn):
                            self.rebalance_table(conn)
                        else:
                            self.shrink.logger.info(f'''Table "{self.db_name}"."{self.schema_name}"."{self.rel_name}" is already rebalanced''')
                        if self.shrink.options.analyze:
                            self.analyze_table(conn)
                    else:
                        self.shrink.logger.info(f'''Table "{self.db_name}"."{self.schema_name}"."{self.rel_name}" doesn't exist, skipping actual rebalance''')

                    dbconn.execSQL(conn, 'COMMIT')

                    inject_fault(f'before_set_status_{self.db_name}.{self.schema_name}.{self.rel_name}')
                    self.shrink.rebalance_schema.setStatusForTableToRebalance(self.db_name, self.schema_name, self.rel_name, self.table_status_after_rebalance)
            else:
                self.shrink.logger.info(f'''DB "{self.db_name}" doesn't exist, skipping actual rebalance for "{self.schema_name}"."{self.rel_name}"''')
            self.shrink.logger.info(f'Complete table rebalance for "{self.db_name}"."{self.schema_name}"."{self.rel_name}"')
            self.set_results(CommandResult(0, b'', b'', True, False))

        def run(self) -> None:
            # Give 2 attempts to process a table. It is needed, when, for example,
            # other session opens a transaction after we have created the rebalance table
            # list, drops the table before we started to rebalance it, and commits the
            # transaction when we've started to rebalance the table.
            attempt_max_cnt = 2
            for i in range(attempt_max_cnt):
                attempt = i + 1
                try:
                    self.process_table(attempt)
                except Exception as e:
                    if attempt < attempt_max_cnt:
                        self.shrink.logger.warning(f"{str(e)}")
                    else:
                        self.shrink.logger.error(f"{str(e)}")
                        self.shrink.logger.error(f'Failed to process the db object "{self.db_name}"."{self.schema_name}"."{self.rel_name}" for {attempt_max_cnt} attempts')
                        if not self.cancel_flag:
                            self.shrink.tables_rebalance_failed = True
                            self.shrink.workers_for_tables_rebalance.haltWork()
                    continue
                break

        def cancel(self):
            super().cancel()
            # there is a very small (but not 0) chance that 'SQLCommand.cancel()' is issued in
            # a tiny time window when we've already checked the flag, but the query hasn't
            # started to execute, so connection.cancel() will fire out without any effect.
            # To handle it, we add the logic below to re-try 'cancel_conn.cancel()' till
            # the flag 'self.long_operation_in_progress' is reset.
            try:
                cnt = 0
                while (self.cancel_conn and
                    not self.cancel_conn.closed and
                    self.long_operation_in_progress):
                    self.cancel_conn.cancel()
                    time.sleep(0.01)
                    cnt += 1
                    if (cnt > 1000):
                        self.shrink.logger.info('TableRebalanceTask cancel timed out')
                        break
            finally:
                pass

    def prepare_shrink_schema(self, is_rollback: bool) -> None:
        status = 'done' if is_rollback else 'none'
        cmp = '<=' if is_rollback else '>'

        dbconn.execSQL(self.conn, 'BEGIN')

        # cleanup list of tables that require rebalance
        # for the case we re-enter this state after we were interrupted right after it
        self.rebalance_schema.clearTablesToRebalanceWithStatus(status)

        cursor = dbconn.query(self.conn, 'SELECT datname FROM pg_database')
        databases_to_process = []
        for record in cursor:
            database_name = record[0]
            if database_name != 'template0':
                databases_to_process.append(database_name)

        if self.rebalance_schema.getProgressType() == self.rebalance_schema.ProgressType.PROGRESS_DETAILED:
            if is_rollback:
                # In case if shrink's rollback, function 'ggrebalance_get_data_move_size()' is used to retrieve
                # the amount of data we need to move. It invokes 'pg_relation_size()' on the segments left after shrink.
                # As during rollback, CTAS approach is used for table redistribution,
                # therefore we will move all the data for the relations with partitioned distribution policy.
                # For the relations with replicated distribution policy, we calculate the size on one segment,
                # and multiply by the number of original segments (as here CTAS is used as well - we will re-create data on each segment).
                str_data_move_size = f'''
                    CASE
                        WHEN p.policytype = 'r' THEN (__ggrebalance_temp_schema.ggrebalance_get_data_move_size(c.oid, p.numsegments) / p.numsegments) * {self.gparray.get_segment_count()}
                        ELSE __ggrebalance_temp_schema.ggrebalance_get_data_move_size(c.oid, p.numsegments)
                    END AS data_move_size'''
            else:
                # In case of normal shrink operation, function 'ggrebalance_get_data_move_size()'
                # behaves a bit different - it calculates data only from the shrunk segments -
                # as for shrink we use INSERT-like operation, not CTAS.
                # And we do not need to move data of the replicated tables.
                str_data_move_size = f'''
                    CASE
                        WHEN p.policytype = 'r' THEN 0
                        ELSE __ggrebalance_temp_schema.ggrebalance_get_data_move_size(c.oid, {self.shrink_plan.getTargetSegmentCount()})
                    END AS data_move_size'''
        else:
            str_data_move_size = '0 as data_move_size'

        for db in databases_to_process:
            dburl = dbconn.DbURL(dbname=db, port=self.gpEnv.getCoordinatorPort())
            with closing(dbconn.connect(dburl, encoding='UTF8')) as conn:
                if self.rebalance_schema.getProgressType() == self.rebalance_schema.ProgressType.PROGRESS_DETAILED:
                    # 'public' schema may be missing, so create our own temp schema to carry the function
                    dbconn.execSQL(conn, 'CREATE SCHEMA __ggrebalance_temp_schema');
                    dbconn.execSQL(conn, f'''
                        CREATE OR REPLACE FUNCTION __ggrebalance_temp_schema.ggrebalance_get_data_move_size(p_rel_oid OID, gp_segment_id_limit INT)
                        RETURNS NUMERIC
                        AS $$
                        WITH cte AS MATERIALIZED (
                            SELECT * FROM gp_dist_random('pg_class') c
                            WHERE c.oid = p_rel_oid
                            AND c.gp_segment_id {'<' if is_rollback else '>='} gp_segment_id_limit
                        ) SELECT sum(pg_catalog.pg_relation_size(cte.oid)) FROM cte;
                        $$ LANGUAGE sql STABLE''')
                cursor = dbconn.query(conn,
                                      f'''SELECT n.nspname, c.relname, c.relkind, pe.writable is not null as external_writable, {str_data_move_size}
                                      FROM pg_class c
                                      JOIN pg_namespace n ON c.relnamespace = n.oid
                                      JOIN gp_distribution_policy p ON c.oid = p.localoid
                                      LEFT JOIN pg_exttable pe on (c.oid=pe.reloid and pe.writable)
                                      WHERE c.relkind IN ('r', 'p', 'm', 'f') AND c.relispartition = FALSE AND
                                      c.relpersistence != 't' AND
                                      p.numsegments {cmp} {self.shrink_plan.getTargetSegmentCount()} AND
                                      n.nspname NOT IN ('pg_catalog', 'information_schema', '{self.rebalance_schema.getSchemaName()}')''')
                for schema_name, rel_name, rel_kind, external_writable, data_move_size in cursor:
                    if rel_kind == 'f' and not external_writable:
                        continue
                    self.rebalance_schema.addTableToRebalance(db, schema_name, rel_name, status, data_move_size)

                if self.rebalance_schema.getProgressType() == self.rebalance_schema.ProgressType.PROGRESS_DETAILED:
                    dbconn.execSQL(conn, 'DROP SCHEMA __ggrebalance_temp_schema CASCADE')

        dbconn.execSQL(self.conn, 'COMMIT')

    def rebalance_tables(self, original_status: str, target_status: str, target_segment_count: int) -> None:
        cursor = self.rebalance_schema.getTablesToRebalanceWithStatus(original_status)

        self.logger.info(f'Tables to process {cursor.rowcount}')

        if cursor.rowcount > 0:
            self.workers_for_tables_rebalance = WorkerPool(numWorkers=min(cursor.rowcount, self.options.parallel))

            for db_name, schema_name, rel_name in cursor:
                task = self.TableRebalanceTask(self,
                                               db_name,
                                               schema_name,
                                               rel_name,
                                               target_segment_count,
                                               target_status)
                self.workers_for_tables_rebalance.addCommand(task)

            self.workers_for_tables_rebalance.join()
            self.workers_for_tables_rebalance.haltWork()
            self.workers_for_tables_rebalance.joinWorkers()
            self.workers_for_tables_rebalance = None

            if self.tables_rebalance_failed:
                raise Exception('failed to redistribute some tables during shrink')

    def state_can_rollback(self, state: str) -> bool:
        if (state in self.states_main_shrink_flow):
            if self.states_main_shrink_flow.index(state) <= self.states_main_shrink_flow.index('STATE_SHRINK_TABLES_DONE'):
                return True
        return False

    def is_gp_segment_configuration_shrinked(self) -> bool:
        if self.dumped_gparray is None:
            return False
        return self.dumped_gparray.get_segment_count() != self.gparray.get_segment_count()

    def shutdown(self) -> None:
        if self.workers_for_tables_rebalance != None:
            self.workers_for_tables_rebalance.haltWork()
            self.workers_for_tables_rebalance.joinWorkers()

        if self.workers_for_segment_stop != None:
            self.workers_for_segment_stop.haltWork()
            self.workers_for_segment_stop.joinWorkers()

        self.shutdown_requested = True
