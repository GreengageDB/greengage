#!/usr/bin/env python3

from transitions import Machine

try:
    from gppylib.commands.unix import *
    from gppylib.commands.gp import *
    from gppylib.gplog import *
    from gppylib.system.environment import *
    from gppylib.system import configurationInterface
    from ggrebalance_modules.planner import *
    from ggrebalance_modules.rebalance_schema import RebalanceSchema, RebalanceSummaryInfo
    from gppylib.fault_injection import *
    from ggrebalance_modules.shrink import GGShrink
    from ggrebalance_modules.ggrebalance_sm import RebalanceSM
    from ggrebalance_modules.rebalance_step import RebalanceStep
    from ggrebalance_modules.rebalance_commons import is_gparray_balanced
except ImportError as e:
    sys.exit('ERROR: Cannot import modules.  Please check that you have sourced greenplum_path.sh.  Detail: ' + str(e))

class GGRebalanceMainSM:

    states_not_logged = [
        'STATE_OPTIONS_VALIDATION',
        'STATE_CLEANUP',
        'STATE_ROLLBACK',
        'STATE_PLANNING_STARTED',
        'STATE_PLANNING_DONE',
        'STATE_CHECK_PREVIOUS_RUN',
        'STATE_END',
        'STATE_ERROR'
    ]

    states_logged = [
        'STATE_START',
        'STATE_SETUP_SCHEMA_STARTED',
        'STATE_SETUP_SCHEMA_DONE',
        'STATE_EXECUTOR_STARTED',
        'STATE_EXECUTOR_DONE',
        'STATE_SHRINK_STARTED',
        'STATE_SHRINK_DONE',
        'STATE_REBALANCE_STARTED',
        'STATE_REBALANCE_DONE',
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
            'trigger': 'move_to_STATE_PLANNING_STARTED',
            'source': 'STATE_OPTIONS_VALIDATION',
            'dest': 'STATE_PLANNING_STARTED'
        },
        {
            'trigger': 'move_to_STATE_PLANNING_DONE',
            'source': 'STATE_PLANNING_STARTED',
            'dest': 'STATE_PLANNING_DONE'
        },
        {
            'trigger': 'move_to_STATE_CHECK_PREVIOUS_RUN',
            'source': 'STATE_PLANNING_DONE',
            'dest': 'STATE_CHECK_PREVIOUS_RUN'
        },
        {
            'trigger': 'move_to_STATE_SETUP_SCHEMA_STARTED',
            'source': 'STATE_CHECK_PREVIOUS_RUN',
            'dest': 'STATE_SETUP_SCHEMA_STARTED'
        },
        {
            'trigger': 'move_to_STATE_SETUP_SCHEMA_DONE',
            'source': 'STATE_SETUP_SCHEMA_STARTED',
            'dest': 'STATE_SETUP_SCHEMA_DONE'
        },
        {
            'trigger': 'move_to_STATE_EXECUTOR_STARTED',
            'source': ['STATE_SETUP_SCHEMA_DONE', 'STATE_CHECK_PREVIOUS_RUN'],
            'dest': 'STATE_EXECUTOR_STARTED'
        },
        {
            'trigger': 'move_to_STATE_SHRINK_STARTED',
            'source': 'STATE_EXECUTOR_STARTED',
            'dest': 'STATE_SHRINK_STARTED'
        },
        {
            'trigger': 'move_to_STATE_SHRINK_DONE',
            'source': 'STATE_SHRINK_STARTED',
            'dest': 'STATE_SHRINK_DONE'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_STARTED',
            'source': ['STATE_EXECUTOR_STARTED', 'STATE_SHRINK_DONE'],
            'dest': 'STATE_REBALANCE_STARTED'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_DONE',
            'source': 'STATE_REBALANCE_STARTED',
            'dest': 'STATE_REBALANCE_DONE'
        },
        {
            'trigger': 'move_to_STATE_EXECUTOR_DONE',
            'source': ['STATE_EXECUTOR_STARTED', 'STATE_SHRINK_DONE', 'STATE_REBALANCE_DONE'],
            'dest': 'STATE_EXECUTOR_DONE'
        },
        {
            'trigger': 'move_to_STATE_END',
            'source': ['STATE_EXECUTOR_DONE', 'STATE_CHECK_PREVIOUS_RUN', 'STATE_CLEANUP', 'STATE_ROLLBACK'],
            'dest': 'STATE_END'
        },
        {
            'trigger': 'move_to_STATE_ERROR',
            'source': '*',
            'dest': 'STATE_ERROR'
        }
    ]

    def __init__(self, conn: dbconn.Connection, logger: Any, dburl: dbconn.DbURL, options: Any, gpEnv: GpCoordinatorEnvironment, gpArray: gparray.GpArray, gpArrayDumpFilename: str):
        self.logger = logger
        self.dburl = dburl
        self.options = options
        self.gparray = gpArray
        self.conn = conn
        self.hard_shutdown = True

        self.rebalance_schema = RebalanceSchema(self.conn)

        self.machine = Machine(model = self,
                               queued=True,
                               states = self.states_not_logged + self.states_logged,
                               transitions = self.transitions,
                               initial = 'STATE_START',
                               before_state_change = 'on_every_state')

        self.gg_shrink = GGShrink(self.conn, self.rebalance_schema, self.logger, self.options, gpEnv, self.gparray, gpArrayDumpFilename)
        self.gg_rebalance = RebalanceSM(self.conn, self.rebalance_schema, self.logger, self.options, self.dburl)

        self.plan = None
        self.main_state_from_prev_run = self.rebalance_schema.getMainStateFromPreviousRun()

        self.shrink_state_from_prev_run = self.rebalance_schema.getShrinkStateFromPreviousRun()
        self.is_shrink_rollback_in_progress = self.gg_shrink.state_is_from_rollback_flow(self.shrink_state_from_prev_run)
        self.prev_shrink_run_was_complete = self.gg_shrink.state_is_final(self.shrink_state_from_prev_run)
        self.is_summary_output_required = False

    def on_every_state(self) -> None:
        if self.state in self.states_logged:
            self.rebalance_schema.storeMainState(self.state)

    def run(self) -> None:
        self.trigger('start')

    def set_hard_shutdown(self, hard_shutdown: bool) -> None:
        self.hard_shutdown = hard_shutdown

    def shutdown(self) -> None:
        if self.hard_shutdown:
            self.logger.info('hard shutdown')
        else:
            self.logger.info('soft shutdown, waiting for critical operations to finish...')

        need_exit = True

        if self.gg_shrink is not None:
            self.gg_shrink.shutdown()
            need_exit = False

        if self.gg_rebalance is not None:
            self.gg_rebalance.shutdown(self.hard_shutdown)
            need_exit = False

        if need_exit:
            sys.exit(1)

    def is_shrink_planned(self) -> bool:
        return isinstance(self.plan, ShrinkPlan)

    def print_shrink_summary(self, summary: RebalanceSummaryInfo) -> None:
        if not self.is_shrink_planned():
            return

        if self.rebalance_schema.getProgressType() == self.rebalance_schema.ProgressType.PROGRESS_NO:
            self.logger.info('Skip final shrink summary report (specify "--simple-progress" or "--detailed-progress" to enable it).')
            return

        self.logger.info('================================================================================')
        self.logger.info('                                   SHRINK                                   ')
        self.logger.info('================================================================================')
        self.logger.info(f'Tables shrunk:\t\t{summary.tables_shrunk}')
        if self.rebalance_schema.getProgressType() == self.rebalance_schema.ProgressType.PROGRESS_DETAILED:
            self.logger.info(f'Bytes processed:\t{summary.bytes_processed}')
            self.logger.info(f'Shrink rate:\t\t{summary.shrink_rate}')
        self.logger.info(f'Shrink total time:\t\t{summary.shrink_total_time}')
        if summary.is_rollback:
            self.logger.info('\n')
            self.logger.info(f'Tables rolled back:\t\t{summary.tables_rolled_back}')
            if self.rebalance_schema.getProgressType() == self.rebalance_schema.ProgressType.PROGRESS_DETAILED:
                self.logger.info(f'Tables rollback rate:\t\t{summary.rollback_rate}')
            self.logger.info(f'Rollback total time:\t\t{summary.rollback_total_time}')

    def print_rebalance_summary(self, summary: RebalanceSummaryInfo) -> None:
        if len(summary.executed_rebalance_steps) == 0:
            return

        segments_moved = 0
        rolled_back_moves = []
        cancelled_moves = []

        for step in summary.executed_rebalance_steps:
            status = step.getStatus()
            if status == RebalanceStep.Status.CANCELLED:
                cancelled_moves.append(step.getMove())
            elif status == RebalanceStep.Status.DONE:
                if step.isRollback():
                    rolled_back_moves.append(step.getMove())
                else:
                    segments_moved += 1
            else:
                raise Exception(f'Unexpected status for executed step: {str(step)}')

        if self.rebalance_schema.getProgressType() == self.rebalance_schema.ProgressType.PROGRESS_NO:
            self.logger.info('Skip final rebalance summary report (specify "--simple-progress" or "--detailed-progress" to enable it).')
        else:
            self.logger.info('================================================================================')
            self.logger.info('                                   REBALANCE                                   ')
            self.logger.info('================================================================================')
            self.logger.info(f'Segments moved:\t\t{segments_moved}')
            self.logger.info(f'Rolled back moves:\t\t{len(rolled_back_moves)}')
            self.logger.info(f'Cancelled moves:\t\t{len(cancelled_moves)}')

        segments_down = []
        gp_array = configurationInterface.getConfigurationProvider().loadSystemConfig(useUtilityMode=False, verbose=False)
        for segment in gp_array.getSegDbList():
            if segment.isSegmentDown():
                segments_down.append(segment)

        balanced = is_gparray_balanced(gp_array)

        # we show warnings regardless the progress type set by user
        show_warnings = len(segments_down) > 0 or len(cancelled_moves) > 0 or not balanced
        if show_warnings:
            self.logger.warning('================================================================================')
            self.logger.warning('                                   WARNINGS                                    ')
            self.logger.warning('================================================================================')

            if len(cancelled_moves) > 0:
                self.logger.warning('------------------------------- Cancelled moves  -------------------------------')
                for move in cancelled_moves:
                    self.logger.warning(str(move))

            if len(segments_down) > 0:
                self.logger.warning('Cluster might be not in fault tolerance mode!')
                self.logger.warning('These segments should be started manually in order cluster to become fault tolerant:')
                for segment in segments_down:
                    self.logger.warning(str(segment))

            if not balanced:
                self.logger.warning('Cluster is left in unbalanced state')
                if len(rolled_back_moves) > 0:
                    self.logger.warning('------------------------------ Rolled back moves ---------------------------------')
                    for move in rolled_back_moves:
                        self.logger.warning(str(move))
                    self.logger.warning('You can review why segments were rolled back and retry rebalance later.')

    def print_full_summary(self) -> None:
        if not self.is_summary_output_required:
            return
        summary = self.rebalance_schema.getProgressSummary()
        self.logger.info('-----------------------------------SUMMARY--------------------------------------')
        self.print_shrink_summary(summary)
        self.print_rebalance_summary(summary)

    # state callbacks start here

    @wrap_func_with_faults
    def on_enter_STATE_OPTIONS_VALIDATION(self) -> None:
        if self.options.clean_required:
            self.trigger('move_to_STATE_CLEANUP')
        elif self.options.rollback_required:
            self.trigger('move_to_STATE_ROLLBACK')
        else:
            self.trigger('move_to_STATE_PLANNING_STARTED')

    @wrap_func_with_faults
    def on_enter_STATE_CLEANUP(self) -> None:
        if not self.rebalance_schema.schemaExists():
            self.logger.info(f"Rebalance schema doesn't exist. Cleanup is not required.")
        else:
            self.plan = self.rebalance_schema.retrieveSavedPlan()
            perform_schema_cleanup = True
            if self.is_shrink_planned():
                perform_schema_cleanup = self.gg_shrink.cleanup(self.prev_shrink_run_was_complete)
            if perform_schema_cleanup:
                self.rebalance_schema.dropSchema()
                self.logger.info('Cleanup is complete')
            else:
                self.logger.info("Cleanup wasn't successfull due to unfinished shrink")
        self.trigger('move_to_STATE_END')

    @wrap_func_with_faults
    def on_enter_STATE_ROLLBACK(self) -> None:
        try:
            self.plan = self.rebalance_schema.retrieveSavedPlan()
            if self.is_shrink_planned():
                if self.is_shrink_rollback_in_progress:
                    self.logger.info("Rollback is already in progress, and was interrupted. Execute 'ggrebalance' without '-r' flag.")
                    return
                if not self.prev_shrink_run_was_complete:
                    self.gg_shrink.rollback(self.plan)
                    self.is_summary_output_required = True
                    return
            self.gg_rebalance.rollback()
            self.is_summary_output_required = True
        finally:
            self.trigger('move_to_STATE_END')

    @wrap_func_with_faults
    def on_enter_STATE_PLANNING_STARTED(self) -> None:
        if self.options.target_segment_count != None:
            self.plan = Planner(self.logger, self.dburl, self.gparray, self.options).plan()

        if self.options.target_segment_count != None and self.options.show_plan:
            self.logger.info(f"Final plan:\n{self.plan}")

        self.trigger('move_to_STATE_PLANNING_DONE')

    @wrap_func_with_faults
    def on_enter_STATE_PLANNING_DONE(self) -> None:
        self.trigger('move_to_STATE_CHECK_PREVIOUS_RUN')

    @wrap_func_with_faults
    def on_enter_STATE_CHECK_PREVIOUS_RUN(self) -> None:
        if not self.rebalance_schema.schemaExists():
            if self.plan == None:
                self.logger.error("Rebalance schema doesn't exists and no shrink plan is supplied. Please specify shrink plan.")
                self.trigger('move_to_STATE_ERROR')
                return
            if self.gparray.get_segment_count() < self.plan.target_segment_count:
                logger.error('Target segment count (%s) > current segment count (%s).\n'
                             'Currently only shrink is supported (target segment count < current segment count).'
                              % (self.plan.target_segment_count, self.gparray.get_segment_count()))
                self.trigger('move_to_STATE_ERROR')
                return
            self.trigger('move_to_STATE_SETUP_SCHEMA_STARTED')
        else:
            # Schema already exists from the previous run.
            # In this case we already have a plan saved in the schema,
            # and we'll continue (or rollback) according to it.
            # Or, if everything is complete, just exit.
            if self.main_state_from_prev_run == 'STATE_EXECUTOR_DONE':
                self.logger.info('Previous run was completed successfully. Please execute cleanup before a new run.')
                return

            if self.plan != None:
                self.logger.error("Can't start a new operation, because the previous one was interrupted. "
                                  "Please try to launch again without a plan to continue from the interrupted state, "
                                  "or use '--rollback' or '--cleanup' options.")
                self.trigger('move_to_STATE_ERROR')
                return

            self.plan = self.rebalance_schema.retrieveSavedPlan()

            self.trigger('move_to_STATE_EXECUTOR_STARTED')

    @wrap_func_with_faults
    def on_enter_STATE_SETUP_SCHEMA_STARTED(self) -> None:
        # Create schema and status tables.
        # It will also save plan in order to use it for recovering after interruption
        if self.options.no_progress:
            progress_type = self.rebalance_schema.ProgressType.PROGRESS_NO
        elif self.options.detailed_progress:
            progress_type = self.rebalance_schema.ProgressType.PROGRESS_DETAILED
        else:
            progress_type = self.rebalance_schema.ProgressType.PROGRESS_SIMPLE
        self.rebalance_schema.createSchema(self.plan, progress_type, self.gg_shrink.states_rollback_flow)
        self.trigger('move_to_STATE_SETUP_SCHEMA_DONE')

    @wrap_func_with_faults
    def on_enter_STATE_SETUP_SCHEMA_DONE(self) -> None:
        self.logger.info(f'Created "{self.rebalance_schema.getSchemaName()}" schema')
        self.trigger('move_to_STATE_EXECUTOR_STARTED')

    @wrap_func_with_faults
    def on_enter_STATE_EXECUTOR_STARTED(self) -> None:
        self.is_summary_output_required = True
        if self.is_shrink_planned():
            if not self.prev_shrink_run_was_complete:
                self.trigger('move_to_STATE_SHRINK_STARTED')
                return
        self.trigger('move_to_STATE_REBALANCE_STARTED')

    @wrap_func_with_faults
    def on_enter_STATE_EXECUTOR_DONE(self) -> None:
        self.trigger('move_to_STATE_END')

    @wrap_func_with_faults
    def on_enter_STATE_SHRINK_STARTED(self) -> None:
        self.gg_shrink.run(self.plan)
        self.trigger('move_to_STATE_SHRINK_DONE')

    @wrap_func_with_faults
    def on_enter_STATE_SHRINK_DONE(self) -> None:
        self.trigger('move_to_STATE_REBALANCE_STARTED')

    @wrap_func_with_faults
    def on_enter_STATE_REBALANCE_STARTED(self) -> None:
        if (self.plan is not None and
            self.plan.getMoves() is not None and
            not self.is_shrink_rollback_in_progress):
            self.gg_rebalance.run(self.plan)

        self.trigger('move_to_STATE_REBALANCE_DONE')

    @wrap_func_with_faults
    def on_enter_STATE_REBALANCE_DONE(self) -> None:
        self.trigger('move_to_STATE_EXECUTOR_DONE')

    @wrap_func_with_faults
    def on_enter_STATE_END(self) -> None:
        self.print_full_summary()

    @wrap_func_with_faults
    def on_enter_STATE_ERROR(self) -> None:
        raise Exception('Main SM entered STATE_ERROR')

    # state callbacks end here
