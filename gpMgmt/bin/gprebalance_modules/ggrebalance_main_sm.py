#!/usr/bin/env python3

from transitions import Machine

try:
    from gppylib.commands.unix import *
    from gppylib.commands.gp import *
    from gppylib.gplog import *
    from gppylib.system.environment import *
    from gprebalance_modules.planner import *
    from gprebalance_modules.rebalance_schema import RebalanceSchema
    from gppylib.fault_injection import *
    from gprebalance_modules.shrink import GGShrink
    from gprebalance_modules.ggrebalance_sm import RebalanceSM
except ImportError as e:
    sys.exit('ERROR: Cannot import modules.  Please check that you have sourced greenplum_path.sh.  Detail: ' + str(e))

class GGRebalanceMainSM:

    states_not_logged = [
        'STATE_START',
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

        self.rebalance_schema = RebalanceSchema(self.conn)

        self.machine = Machine(model = self,
                               queued=True,
                               states = self.states_not_logged + self.states_logged,
                               transitions = self.transitions,
                               initial = 'STATE_START',
                               before_state_change = 'on_every_state')

        self.gg_shrink = GGShrink(self.conn, self.rebalance_schema, self.logger, self.options, gpEnv, self.gparray, gpArrayDumpFilename)
        self.gg_rebalance = RebalanceSM(self.conn, self.rebalance_schema, self.logger, self.options, self.gparray)

        self.plan = None
        self.main_state_from_prev_run = self.rebalance_schema.getMainStateFromPreviousRun()

    def on_every_state(self) -> None:
        if self.state in self.states_logged:
            self.rebalance_schema.storeMainState(self.state)

    def run(self) -> None:
        self.trigger('start')

    def shutdown(self) -> None:
        need_exit = True

        if self.gg_shrink is not None:
            self.gg_shrink.shutdown()
            need_exit = False

        if self.gg_rebalance is not None:
            self.gg_rebalance.shutdown()
            need_exit = False

        if need_exit:
            sys.exit(1)

    # state callbacks start here

    @wrap_state_func_with_faults
    def on_enter_STATE_OPTIONS_VALIDATION(self) -> None:
        if self.options.clean_required:
            self.trigger('move_to_STATE_CLEANUP')
        elif self.options.rollback_required:
            self.trigger('move_to_STATE_ROLLBACK')
        else:
            self.trigger('move_to_STATE_PLANNING_STARTED')

    @wrap_state_func_with_faults
    def on_enter_STATE_CLEANUP(self) -> None:
        if not self.rebalance_schema.schemaExists():
            self.logger.info(f"Rebalance schema doesn't exist. Cleanup is not required.")
        else:
            prev_run_was_complete = (self.main_state_from_prev_run == 'STATE_EXECUTOR_DONE' or
                                     self.main_state_from_prev_run == 'STATE_ROLLBACK')
            self.gg_shrink.cleanup(prev_run_was_complete)
            self.rebalance_schema.dropSchema()
            self.logger.info('Cleanup is complete')
        self.trigger('move_to_STATE_END')

    @wrap_state_func_with_faults
    def on_enter_STATE_ROLLBACK(self) -> None:
        self.plan = self.rebalance_schema.retrieveSavedPlan()
        self.gg_shrink.rollback(self.plan)
        self.trigger('move_to_STATE_END')

    @wrap_state_func_with_faults
    def on_enter_STATE_PLANNING_STARTED(self) -> None:
        if self.options.target_segment_count != None:
            self.plan = Planner(self.logger, self.dburl, self.gparray, self.options).plan()

        if self.options.target_segment_count != None and self.options.show_plan:
            self.logger.info(f"Final plan:\n{self.plan}")

        self.trigger('move_to_STATE_PLANNING_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_PLANNING_DONE(self) -> None:
        self.trigger('move_to_STATE_CHECK_PREVIOUS_RUN')

    @wrap_state_func_with_faults
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

    @wrap_state_func_with_faults
    def on_enter_STATE_SETUP_SCHEMA_STARTED(self) -> None:
        # Create schema and status tables.
        # It will also save plan in order to use it for recovering after interruption
        self.rebalance_schema.createSchema(self.plan)
        self.trigger('move_to_STATE_SETUP_SCHEMA_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_SETUP_SCHEMA_DONE(self) -> None:
        self.logger.info(f'Created "{self.rebalance_schema.getSchemaName()}" schema')
        self.trigger('move_to_STATE_EXECUTOR_STARTED')

    @wrap_state_func_with_faults
    def on_enter_STATE_EXECUTOR_STARTED(self) -> None:
        if isinstance(self.plan, ShrinkPlan):
            shrink_state_from_prev_run = self.rebalance_schema.getShrinkStateFromPreviousRun()
            if not self.gg_shrink.state_is_final(shrink_state_from_prev_run):
                self.trigger('move_to_STATE_SHRINK_STARTED')
                return
        self.trigger('move_to_STATE_REBALANCE_STARTED')

    @wrap_state_func_with_faults
    def on_enter_STATE_EXECUTOR_DONE(self) -> None:
        self.trigger('move_to_STATE_END')

    @wrap_state_func_with_faults
    def on_enter_STATE_SHRINK_STARTED(self) -> None:
        self.gg_shrink.run(self.plan)
        self.trigger('move_to_STATE_SHRINK_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_SHRINK_DONE(self) -> None:
        self.trigger('move_to_STATE_REBALANCE_STARTED')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_STARTED(self) -> None:
        if self.plan is not None and self.plan.getMoves() is not None:
            self.gg_rebalance.run(self.plan)
            self.logger.info('Rebalance is complete')

        self.trigger('move_to_STATE_REBALANCE_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_DONE(self) -> None:
        self.trigger('move_to_STATE_EXECUTOR_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_END(self) -> None:
        pass

    @wrap_state_func_with_faults
    def on_enter_STATE_ERROR(self) -> None:
        raise Exception('Main SM entered STATE_ERROR')

    # state callbacks end here
