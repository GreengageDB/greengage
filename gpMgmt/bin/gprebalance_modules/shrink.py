#!/usr/bin/env python3

from transitions import Machine
from contextlib import closing
from typing import Any

try:
    from gppylib.commands.unix import *
    from gppylib.commands.gp import *
    from gppylib.gplog import *
    from gppylib.db import dbconn
    from gppylib import userinput
    from gppylib.gparray import GpArray, Segment
    from gppylib.fault_injection import *
    from gppylib.userinput import *
    from gppylib.commands import base
    from gppylib.commands.gp import SEGMENT_STOP_TIMEOUT_DEFAULT, SegmentStop
    from gppylib.system.environment import *
    from gprebalance_modules.planner import *
    from gprebalance_modules.rebalance_commons import RebalanceSchema
except ImportError as e:
    sys.exit('ERROR: Cannot import modules.  Please check that you have sourced greenplum_path.sh.  Detail: ' + str(e))


DBNAME = 'postgres'

def print_progress(pool: WorkerPool, interval: int = 10) -> None:
    """
    Waits for a WorkerPool to complete, printing a progress percentage marker
    once at the beginning of the call, and thereafter at the provided interval
    (default ten seconds). A final 100% marker is printed upon completion.
    """
    def print_completed_percentage() -> bool:
        # pool.completed can change asynchronously; save its value.
        completed = pool.completed

        pct = 0
        if pool.assigned:
            pct = float(completed) / pool.assigned

        pool.logger.info(f'{pct:.2%} of jobs completed')
        return completed >= pool.assigned

    # print_completed_percentage() returns True if we're done.
    while not print_completed_percentage():
        if pool.join(interval):
            return

class GGShrink:
    timeout = SEGMENT_STOP_TIMEOUT_DEFAULT
    stop_mode = 'fast'

    states = [
        'STATE_START',
        'STATE_OPTIONS_VALIDATION',
        'STATE_CHECK_PREVIOUS_RUN',
        'STATE_END',
        'STATE_CLEANUP',
        'STATE_ERROR',
        'STATE_ROLLBACK',
        'STATE_END_FROM_ROLLBACK'
    ]

    # Note: order of states in the list below is important,
    # as we rely on it when recover from an interrupted state.
    # These states are separated from 'states' above, because
    # these states exist after the shrink schema is created,
    # so they are reflected in the status table inside the schema,
    # and we can re-enter the interrupted state.
    states_main_shrink_flow = [
        'STATE_SETUP_SHRINK_SCHEMA_STARTED',
        'STATE_SETUP_SHRINK_SCHEMA_DONE',
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
            'dest': 'STATE_OPTIONS_VALIDATION'
        },
        {
            'trigger': 'move_to_STATE_CLEANUP',
            'source': 'STATE_OPTIONS_VALIDATION',
            'dest': 'STATE_CLEANUP'
        },
        {
            'trigger': 'move_to_STATE_ROLLBACK',
            'source': 'STATE_OPTIONS_VALIDATION',
            'dest': 'STATE_ROLLBACK'
        },
        {
            'trigger': 'move_to_STATE_CHECK_PREVIOUS_RUN',
            'source': ['STATE_OPTIONS_VALIDATION', 'STATE_ROLLBACK'],
            'dest': 'STATE_CHECK_PREVIOUS_RUN'
        },
        {
            'trigger': 'move_to_STATE_SETUP_SHRINK_SCHEMA_STARTED',
            'source': 'STATE_CHECK_PREVIOUS_RUN',
            'dest': 'STATE_SETUP_SHRINK_SCHEMA_STARTED'
        },
        {
            'trigger': 'move_to_STATE_SETUP_SHRINK_SCHEMA_DONE',
            'source': 'STATE_SETUP_SHRINK_SCHEMA_STARTED',
            'dest': 'STATE_SETUP_SHRINK_SCHEMA_DONE'
        },
        {
            'trigger': 'move_to_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_STARTED',
            'source': 'STATE_SETUP_SHRINK_SCHEMA_DONE',
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
            'source': ['STATE_SHRINK_DONE', 'STATE_CHECK_PREVIOUS_RUN', 'STATE_CLEANUP', 'STATE_END_FROM_ROLLBACK'],
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
            'source': ['STATE_SHRINK_ROLLBACK_DROP_SCHEMA_DONE', 'STATE_ROLLBACK', 'STATE_CHECK_PREVIOUS_RUN'],
            'dest': 'STATE_END_FROM_ROLLBACK'
        },
        {
            'trigger': 'move_to_STATE_ERROR',
            'source': '*',
            'dest': 'STATE_ERROR'
        }
    ]

    def __init__(self, logger: Any, dburl: dbconn.DbURL, options: Any, gpEnv: GpCoordinatorEnvironment, gpArray: gparray.GpArray, gpArrayDumpFilename: str) -> None:
        self.logger = logger
        self.dburl = dburl
        self.options = options
        self.gpEnv = gpEnv
        self.conn = dbconn.connect(
            self.dburl, encoding='UTF8', allowSystemTableMods=True)
        self.shutdown_requested = False
        self.workers_for_tables_rebalance = None
        self.workers_for_segment_stop = None
        self.gparray = gpArray
        self.gparray_dump_file = gpArrayDumpFilename
        self.rebalance_schema = RebalanceSchema(self.conn)
        self.shrink_plan = None
        self.needs_repopulate = False
        self.dumped_gparray = gparray.GpArray.initFromFile(self.gparray_dump_file) if os.path.exists(self.gparray_dump_file) else None

        self.machine = Machine(model = self,
                               queued=True,
                               states = self.states + self.states_main_shrink_flow + self.states_rollback_flow,
                               transitions = self.transitions,
                               initial = 'STATE_START',
                               before_state_change = 'on_every_state')

    def run(self, shrinkPlan: Plan) -> None:
        self.shrink_plan = shrinkPlan
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
                self.needs_repopulate = True
                return 'STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_STARTED'
        
        return self.states_main_shrink_flow[prev_idx + 1]

    def on_every_state(self) -> None:
        if self.shutdown_requested:
            self.logger.info('Shrink was interrupted')
            raise Exception('Shrink was interrupted')

        assert self.state in self.states + self.states_main_shrink_flow + self.states_rollback_flow

        if self.state in self.states_main_shrink_flow + self.states_rollback_flow:
            self.rebalance_schema.storeState(self.state)

    # state callbacks start here
    @wrap_state_func_with_faults
    def on_enter_STATE_OPTIONS_VALIDATION(self) -> None:
        if self.options.clean_required:
            self.trigger('move_to_STATE_CLEANUP')
        elif self.options.rollback_required:
            self.trigger('move_to_STATE_ROLLBACK')
        else:
            self.trigger('move_to_STATE_CHECK_PREVIOUS_RUN')

    @wrap_state_func_with_faults
    def on_enter_STATE_CHECK_PREVIOUS_RUN(self) -> None:
        # check if rebalance schema exists
        # and whether we can get the state where we stopped in previous run
        # in order to proceed from the same point
        if self.rebalance_schema.schemaExists():
            self.logger.info('Rebalance schema exists')
            state_from_prev_run = self.rebalance_schema.getStateFromPreviousRun()
            # check maybe the state is the final one
            if state_from_prev_run == self.states_main_shrink_flow[-1]:
                if self.options.rollback_required:
                    self.logger.info(f"Previous run was completed successfully. Can't perform rollback.")
                    self.trigger('move_to_STATE_END_FROM_ROLLBACK')
                else:
                    self.logger.error('Previous run was completed successfully. Please execute cleanup before a new run.')
                    self.trigger('move_to_STATE_END')
                return

            elif self.shrink_plan != None:
                self.logger.error("Can't start a new operation, because the previous one was interrupted. "
                                  "Please try to launch again without a plan to continue from the interrupted state, "
                                  "or use '--rollback' or '--cleanup' options.")
                self.trigger('move_to_STATE_ERROR')
                return

            else:
                self.shrink_plan = self.rebalance_schema.retrieveSavedPlan()
                if self.shrink_plan == None:
                    self.logger.error('No saved plan found. Try to execute cleanup.')
                    self.trigger('move_to_STATE_ERROR')
                    return

                if state_from_prev_run in self.states_rollback_flow:
                    self.logger.info('Continue interrupted rollback operation...')
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

                    self.logger.info('Continue interrupted operation...')
                    self.logger.info(f"Previous run stopped after state '{state_from_prev_run}', trying to continue from the next state...")
                    try:
                        next_state = self.get_state_after_interrupt(state_from_prev_run)
                    except:
                        self.logger.error("Can't determine next state. Try to execute cleanup.")
                        self.trigger('move_to_STATE_ERROR')
                        return

                # use auto to_«state» method to recover
                self.trigger(f'to_{next_state}')
        else:
            if self.shrink_plan == None:
                self.logger.error("Rebalance schema doesn't exists and no shrink plan is supplied. Please specify shrink plan.")
                self.trigger('move_to_STATE_ERROR')
                return
            if self.gparray.get_segment_count() <= self.shrink_plan.target_segment_count:
                logger.error('Target segment count (%s) >= current segment count (%s).\n'
                             'Currently only shrink is supported (target segment count < current segment count).'
                              % (self.shrink_plan.target_segment_count, self.gparray.get_segment_count()))
                self.trigger('move_to_STATE_ERROR')
                return

            self.trigger('move_to_STATE_SETUP_SHRINK_SCHEMA_STARTED')

    @wrap_state_func_with_faults
    def on_enter_STATE_SETUP_SHRINK_SCHEMA_STARTED(self) -> None:
        # Create schema and status tables.
        # It will also save plan in order to use it for recovering after interruption
        self.rebalance_schema.createSchema(self.shrink_plan)
        self.trigger('move_to_STATE_SETUP_SHRINK_SCHEMA_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_SETUP_SHRINK_SCHEMA_DONE(self) -> None:
        self.logger.info(f'Created "{self.rebalance_schema.getSchemaName()}" schema')
        self.trigger('move_to_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_STARTED')

    @wrap_state_func_with_faults
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

    @wrap_state_func_with_faults
    def on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_DONE(self) -> None:
        self.logger.info(f'Updated target segment count to {self.shrink_plan.getTargetSegmentCount()}')
        self.trigger('move_to_STATE_PREPARE_SHRINK_SCHEMA_STARTED')

    @wrap_state_func_with_faults
    def on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED(self) -> None:
        self.prepare_shrink_schema(False)
        self.trigger('move_to_STATE_PREPARE_SHRINK_SCHEMA_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_PREPARE_SHRINK_SCHEMA_DONE(self) -> None:
        self.logger.info(f'Initiated list of tables to rebalance')
        self.trigger('move_to_STATE_SHRINK_TABLES_STARTED')

    @wrap_state_func_with_faults
    def on_enter_STATE_SHRINK_TABLES_STARTED(self) -> None:
        self.logger.info('Start tables rebalance for shrink')
        # perform 'ALTER TABLE REBALANCE' for all not yet processed tables
        self.rebalance_tables('none', 'done', self.shrink_plan.getTargetSegmentCount())
        self.trigger('move_to_STATE_SHRINK_TABLES_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_SHRINK_TABLES_DONE(self) -> None:
        self.logger.info('Tables rebalance complete')
        self.trigger('move_to_STATE_SHRINK_CATALOG_STARTED')

    @wrap_state_func_with_faults
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

    @wrap_state_func_with_faults
    def on_enter_STATE_SHRINK_CATALOG_DONE(self) -> None:
        self.logger.info('Catalog shrink complete')
        self.trigger('move_to_STATE_SHRINK_SEGMENTS_STOP_STARTED')

    @wrap_state_func_with_faults
    def on_enter_STATE_SHRINK_SEGMENTS_STOP_STARTED(self) -> None:
        self.logger.info('Stopping shrinked segments...')

        gp_array = self.dumped_gparray

        if gp_array is None:
            gp_array = self.gparray
        
        segments_to_stop = gp_array.get_segment_count() - self.shrink_plan.getTargetSegmentCount()
        segments_to_stop = segments_to_stop * 2 # consider mirrors
        self.workers_for_segment_stop = WorkerPool(numWorkers=min(segments_to_stop, self.options.batch_size))

        for seg_pair in gp_array.getSegmentList():
            primary_seg = seg_pair.primaryDB
            mirror_seg = seg_pair.mirrorDB
            if primary_seg.getSegmentContentId() >= self.shrink_plan.getTargetSegmentCount():
                if primary_seg.isSegmentUp():
                    cmd = self.SegmentStopAfterShrink(self, primary_seg)
                    self.workers_for_segment_stop.addCommand(cmd)

                if mirror_seg != None and mirror_seg.isSegmentUp():
                    cmd = self.SegmentStopAfterShrink(self, mirror_seg)
                    self.workers_for_segment_stop.addCommand(cmd)

        print_progress(self.workers_for_segment_stop, interval=1)

        self.workers_for_segment_stop.haltWork()
        self.workers_for_segment_stop.joinWorkers()

        for task in self.workers_for_segment_stop.getCompletedItems():
            if not task.was_successful():
                raise Exception('Failed to stop segments')

        self.workers_for_segment_stop = None

        self.trigger('move_to_STATE_SHRINK_SEGMENTS_STOP_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_SHRINK_SEGMENTS_STOP_DONE(self) -> None:
        self.logger.info('Shrinked segments were stopped')
        self.trigger('move_to_STATE_SHRINK_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_SHRINK_DONE(self) -> None:
        os.remove(self.gparray_dump_file)
        self.logger.info('Shrink is complete')
        self.trigger('move_to_STATE_END')

    @wrap_state_func_with_faults
    def on_enter_STATE_CLEANUP(self) -> None:
        if not self.rebalance_schema.schemaExists():
            self.logger.info(f"Rebalance schema doesn't exist. Cleanup is not required.")
        else:
            state_from_prev_run = self.rebalance_schema.getStateFromPreviousRun()
            if state_from_prev_run != self.states_main_shrink_flow[-1]:
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

                if (self.options.interactive and
                    not userinput.ask_yesno(None, "\nContinue with cleanup?", 'Y')):
                    self.logger.info('Cleanup was interrupted...')
                    self.trigger('move_to_STATE_END')
                    return

                if (numsegments_is_set and
                    (not self.options.interactive or userinput.ask_yesno(None, "\nReset numsegments to default?", 'Y'))):
                    dbconn.execSQL(self.conn, 'BEGIN')
                    dbconn.execSQL(self.conn, 'SELECT gp_expand_lock_catalog()')
                    dbconn.execSQL(self.conn, 'SELECT gp_toolkit.gp_reset_rebalance_numsegments()')
                    dbconn.execSQL(self.conn, 'COMMIT')
                    self.logger.info('Reset numsegments to default is done.')

            if os.path.exists(self.gparray_dump_file):
                os.remove(self.gparray_dump_file)
            self.rebalance_schema.dropSchema()
            self.logger.info('Cleanup is complete')
        self.trigger('move_to_STATE_END')

    @wrap_state_func_with_faults
    def on_enter_STATE_ROLLBACK(self) -> None:
        if not self.rebalance_schema.schemaExists():
            self.logger.info("Rebalance schema doesn't exist. Can't perform rollback.")
            self.trigger('move_to_STATE_END_FROM_ROLLBACK')
            return
        else:
            state_from_prev_run = self.rebalance_schema.getStateFromPreviousRun()
            if state_from_prev_run != 'not defined':
                # check maybe the state is the final one
                if state_from_prev_run == self.states_main_shrink_flow[-1]:
                    self.logger.info("Previous run was completed successfully. Can't perform rollback.")
                    self.trigger('move_to_STATE_END_FROM_ROLLBACK')
                    return

                if not self.state_can_rollback(state_from_prev_run) or self.is_gp_segment_configuration_shrinked():
                    self.logger.info("Can't perform rollback as the catalog is already updated")
                    self.trigger('move_to_STATE_END_FROM_ROLLBACK')
                    return

        self.trigger('move_to_STATE_CHECK_PREVIOUS_RUN')

    @wrap_state_func_with_faults
    def on_enter_STATE_SHRINK_ROLLBACK_RESTORE_TARGET_SEGMENT_COUNT_START(self) -> None:
        dbconn.execSQL(self.conn, 'BEGIN')
        dbconn.execSQL(self.conn, 'SELECT gp_expand_lock_catalog()')
        dbconn.execSQL(self.conn, 'SELECT gp_toolkit.gp_reset_rebalance_numsegments()')
        # Store state here in case we fail before we enter 'on_every_state()'
        # because after COMMIT we are on a one-way road of rollback.
        self.rebalance_schema.storeState(self.state)
        dbconn.execSQL(self.conn, 'COMMIT')

        self.trigger('move_to_STATE_SHRINK_ROLLBACK_RESTORE_TARGET_SEGMENT_COUNT_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_SHRINK_ROLLBACK_RESTORE_TARGET_SEGMENT_COUNT_DONE(self) -> None:
        self.trigger('move_to_STATE_SHRINK_ROLLBACK_PREPARE_SCHEMA_START')

    @wrap_state_func_with_faults
    def on_enter_STATE_SHRINK_ROLLBACK_PREPARE_SCHEMA_START(self) -> None:
        self.prepare_shrink_schema(True)
        self.trigger('move_to_STATE_SHRINK_ROLLBACK_PREPARE_SCHEMA_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_SHRINK_ROLLBACK_PREPARE_SCHEMA_DONE(self) -> None:
        self.trigger('move_to_STATE_SHRINK_ROLLBACK_SHRINKED_TABLES_START')

    @wrap_state_func_with_faults
    def on_enter_STATE_SHRINK_ROLLBACK_SHRINKED_TABLES_START(self) -> None:
        self.logger.info('Start tables rebalance for rollback')
        # perform 'ALTER TABLE REBALANCE' for all not yet processed tables
        self.rebalance_tables('done', 'none', self.gparray.get_segment_count())
        self.trigger('move_to_STATE_SHRINK_ROLLBACK_SHRINKED_TABLES_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_SHRINK_ROLLBACK_SHRINKED_TABLES_DONE(self) -> None:
        self.trigger('move_to_STATE_SHRINK_ROLLBACK_DROP_SCHEMA_START')

    @wrap_state_func_with_faults
    def on_enter_STATE_SHRINK_ROLLBACK_DROP_SCHEMA_START(self) -> None:
        if os.path.exists(self.gparray_dump_file):
            os.remove(self.gparray_dump_file)
        self.rebalance_schema.dropSchema()
        self.trigger('move_to_STATE_SHRINK_ROLLBACK_DROP_SCHEMA_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_SHRINK_ROLLBACK_DROP_SCHEMA_DONE(self) -> None:
        self.logger.info('Rollback is complete.')
        self.trigger('move_to_STATE_END_FROM_ROLLBACK')

    @wrap_state_func_with_faults
    def on_enter_STATE_END_FROM_ROLLBACK(self) -> None:
        self.trigger('move_to_STATE_END')

    @wrap_state_func_with_faults
    def on_enter_STATE_END(self) -> None:
        self.conn.close()

    @wrap_state_func_with_faults
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
                inject_fault(f'fault_segment_stop_dbid_{self.segment.getSegmentDbId()}')
                fun(self)
            return func_with_faults

        @wrap_segment_stop_with_faults
        def run(self) -> None:
            self.shrink.logger.info(f'Stopping shrinked segment dbid {self.segment.getSegmentDbId()} @ host={self.remoteHost}, datadir={self.segment.getSegmentDataDirectory()}')
            self.checkRunningSegment.run()
            if self.checkRunningSegment.is_shutdown():
                self.shrink.logger.info(f'Segment dbid {self.segment.getSegmentDbId()} is already down @ host={self.remoteHost}, datadir={self.segment.getSegmentDataDirectory()} ')
                self.set_results(CommandResult(0, b'', b'', True, False))
            else:
                SegmentStop.run(self)
                self.shrink.logger.info(f'Stopped shrinked segment dbid {self.segment.getSegmentDbId()} @ host={self.remoteHost}, datadir={self.segment.getSegmentDataDirectory()}')

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
            SQLCommand.__init__(self, f'task rebalance for {self.db_name}.{self.schema_name}.{self.rel_name}')

        # decorator to inject a fault before running TableRebalanceTask for a specific {db_name, schema_name, rel_name}
        def wrap_table_rebalance_with_faults(fun):
            def func_with_faults(self):
                inject_fault(f'fault_rebalance_table_{self.db_name}.{self.schema_name}.{self.rel_name}')
                fun(self)
            return func_with_faults

        @wrap_table_rebalance_with_faults
        def run(self) -> None:
            self.shrink.logger.info(f'Start table rebalance for "{self.db_name}"."{self.schema_name}"."{self.rel_name}" to {self.target_segment_count} segments')
            dburl = dbconn.DbURL(dbname=self.db_name, port=self.shrink.gpEnv.getCoordinatorPort())
            with closing(dbconn.connect(dburl, encoding='UTF8')) as conn:
                dbconn.execSQL(conn, 'BEGIN')
                dbconn.execSQL(conn,
                               f'''ALTER TABLE "{self.schema_name}"."{self.rel_name}"
                               REBALANCE {self.target_segment_count}''')
                self.shrink.rebalance_schema.setStatusForTableToRebalance(self.db_name, self.schema_name, self.rel_name, self.table_status_after_rebalance)
                dbconn.execSQL(conn, 'COMMIT')
            self.shrink.logger.info(f'Complete table rebalance for "{self.db_name}"."{self.schema_name}"."{self.rel_name}"')
            self.set_results(CommandResult(0, b'', b'', True, False))

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

        for db in databases_to_process:
            dburl = dbconn.DbURL(dbname=db, port=self.gpEnv.getCoordinatorPort())
            with closing(dbconn.connect(dburl, encoding='UTF8')) as conn:
                cursor = dbconn.query(conn,
                                      f'''SELECT n.nspname, c.relname
                                      FROM pg_class c
                                      JOIN pg_namespace n ON c.relnamespace = n.oid
                                      JOIN gp_distribution_policy p ON c.oid = p.localoid
                                      WHERE c.relkind IN ('r', 'p') AND c.relispartition = FALSE AND
                                      p.numsegments {cmp} {self.shrink_plan.getTargetSegmentCount()} AND
                                      n.nspname NOT IN ('pg_catalog', 'information_schema', '{self.rebalance_schema.getSchemaName()}')''')
                for schema_name, rel_name in cursor:
                    self.rebalance_schema.addTableToRebalance(db, schema_name, rel_name, status)

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

            print_progress(self.workers_for_tables_rebalance, interval=1)

            self.workers_for_tables_rebalance.haltWork()
            self.workers_for_tables_rebalance.joinWorkers()

            for task in self.workers_for_tables_rebalance.getCompletedItems():
                if not task.was_successful():
                    raise Exception(f'Failed to do ALTER REBALANCE: {task.get_results().stderr}')

            self.workers_for_tables_rebalance = None

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
