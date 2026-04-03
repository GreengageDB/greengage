#!/usr/bin/env python3

from transitions import Machine
from enum import Enum

try:
    from gppylib.commands.unix import *
    from gppylib.commands.gp import *
    from gppylib.gplog import *
    from gppylib.commands.gp import GpMoveMirrors, SegmentStatus, GpConfigHelper
    from gppylib.system.environment import *
    from ggrebalance_modules.planner import *
    from ggrebalance_modules.rebalance_schema import RebalanceSchema, STATE_NOT_DEFINED
    from ggrebalance_modules.rebalance_step import *
    from ggrebalance_modules.rebalance_commons import interactive_check_yesno
    from gppylib.fault_injection import *
except ImportError as e:
    sys.exit('ERROR: Cannot import modules.  Please check that you have sourced greenplum_path.sh.  Detail: ' + str(e))

class RebalanceSM:

    states_not_logged = [
        'STATE_REBALANCE_INIT',
        'STATE_CHECK_PREVIOUS_RUN',
        'STATE_ERROR'
    ]

    states_main_rebalance_flow = [
        'STATE_REBALANCE_STARTED',
        'STATE_REBALANCE_PREPARE_MOVES_STARTED',
        'STATE_REBALANCE_PREPARE_MOVES_DONE',
        'STATE_REBALANCE_EXECUTION_STARTED',
        'STATE_REBALANCE_MOVES_SUCCEEDED',
        'STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_STARTED',
        'STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_DONE',
        'STATE_REBALANCE_EXECUTION_DONE',
        'STATE_REBALANCE_DONE'
    ]

    states_rollback_rebalance_flow = [
        'STATE_REBALANCE_ROLLBACK_STARTED',
        'STATE_REBALANCE_ROLLBACK_PREPARE_MOVES_STARTED',
        'STATE_REBALANCE_ROLLBACK_PREPARE_MOVES_DONE'
    ]

    transitions = [
        {
            'trigger': 'start',
            'source': 'STATE_REBALANCE_INIT',
            'dest': 'STATE_CHECK_PREVIOUS_RUN'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_STARTED',
            'source': 'STATE_CHECK_PREVIOUS_RUN',
            'dest': 'STATE_REBALANCE_STARTED'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_PREPARE_MOVES_STARTED',
            'source': 'STATE_REBALANCE_STARTED',
            'dest': 'STATE_REBALANCE_PREPARE_MOVES_STARTED'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_PREPARE_MOVES_DONE',
            'source': 'STATE_REBALANCE_PREPARE_MOVES_STARTED',
            'dest': 'STATE_REBALANCE_PREPARE_MOVES_DONE'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_EXECUTION_STARTED',
            'source': ['STATE_REBALANCE_PREPARE_MOVES_DONE',
                       'STATE_REBALANCE_MOVES_SUCCEEDED',
                       'STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_DONE',
                       'STATE_REBALANCE_ROLLBACK_PREPARE_MOVES_DONE'],
            'dest': 'STATE_REBALANCE_EXECUTION_STARTED'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_MOVES_SUCCEEDED',
            'source': 'STATE_REBALANCE_EXECUTION_STARTED',
            'dest': 'STATE_REBALANCE_MOVES_SUCCEEDED'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_STARTED',
            'source': 'STATE_REBALANCE_EXECUTION_STARTED',
            'dest': 'STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_STARTED'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_DONE',
            'source': 'STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_STARTED',
            'dest': 'STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_DONE'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_EXECUTION_DONE',
            'source': 'STATE_REBALANCE_EXECUTION_STARTED',
            'dest': 'STATE_REBALANCE_EXECUTION_DONE'
        },
        {
            'trigger': 'rollback',
            'source': 'STATE_REBALANCE_INIT',
            'dest': 'STATE_REBALANCE_ROLLBACK_STARTED'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_ROLLBACK_PREPARE_MOVES_STARTED',
            'source': 'STATE_REBALANCE_ROLLBACK_STARTED',
            'dest': 'STATE_REBALANCE_ROLLBACK_PREPARE_MOVES_STARTED'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_ROLLBACK_PREPARE_MOVES_DONE',
            'source': 'STATE_REBALANCE_ROLLBACK_PREPARE_MOVES_STARTED',
            'dest': 'STATE_REBALANCE_ROLLBACK_PREPARE_MOVES_DONE'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_DONE',
            'source': 'STATE_REBALANCE_EXECUTION_DONE',
            'dest': 'STATE_REBALANCE_DONE'
        },
        {
            'trigger': 'move_to_STATE_ERROR',
            'source': '*',
            'dest': 'STATE_ERROR'
        }
    ]

    class RoleSwapDirection(Enum):
        PRIMARY_TO_MIRROR = 1
        MIRROR_TO_PRIMARY = 2

    def __init__(self, conn: dbconn.Connection, schema: RebalanceSchema, logger: Any, options: Any, dburl: dbconn.DbURL):
        self.logger = logger
        self.options = options
        self.shutdown_requested = False
        self.dburl = dburl
        self.conn = conn
        self.rebalance_schema = schema
        self.cmd = None
        self.is_rollback_flow = False

        self.machine = Machine(model = self,
                               queued=True,
                               states = self.states_main_rebalance_flow + self.states_not_logged + self.states_rollback_rebalance_flow,
                               transitions = self.transitions,
                               initial = 'STATE_REBALANCE_INIT',
                               before_state_change = 'on_every_state')

    def on_every_state(self) -> None:
        if self.shutdown_requested:
            self.logger.info('Rebalance was interrupted')
            raise Exception('Rebalance was interrupted')

        if self.state in self.states_main_rebalance_flow + self.states_rollback_rebalance_flow:
            self.rebalance_schema.storeRebalanceState(self.state)

    def run(self, plan: Plan) -> None:
        self.rebalance_plan = plan
        if not self.rebalance_plan.getMoves():
            return

        self.moves_primaries = []
        self.moves_mirrors = []
        for move in self.rebalance_plan.getMoves():
            if move.seg.isSegmentPrimary() :
                self.moves_primaries.append(move)
            else:
                self.moves_mirrors.append(move)
        
        self.primary_segments_to_move = [move.seg for move in self.moves_primaries]

        self.trigger('start')

    def rollback(self) -> None:
        if self.rebalance_schema.schemaExists():
            self.trigger('rollback')
        else:
            self.logger.info("Rebalance schema doesn't exist. Can't perform rollback.")

    def process_moves(self, steps: List[RebalanceStepMoveMirror]):
        if len(steps) == 0:
            return

        filename = self.create_config_file(steps)
        gpmovemirrors_options = f'--skip-resource-estimation -a -i {filename}'

        if self.options.parallel is not None:
            batch_size = self.options.parallel
            # gpmovemirrors has its own limitation for batch size,
            # need to consider it here.
            if batch_size > MAX_COORDINATOR_NUM_WORKERS:
                batch_size = MAX_COORDINATOR_NUM_WORKERS
            gpmovemirrors_options += f' -B {batch_size}'
            if self.options.hba_hostnames:
                gpmovemirrors_options = gpmovemirrors_options + ' --hba-hostnames'
            if self.options.logfile_directory is not None:
                gpmovemirrors_options = gpmovemirrors_options + f' -l "{str(self.options.logfile_directory)}"'
        try:
            self.cmd = GpMoveMirrors("Running gpmovemirrors", options=gpmovemirrors_options)
            self.cmd.run(validateAfter=True)
        except Exception as e:
            logger.error(str(e))
            error_msg = f"Failed to execute 'gpmovemirrors {gpmovemirrors_options}'"
            raise Exception(error_msg)
        finally:
            self.cmd = None

        if os.path.exists(filename):
            os.remove(filename)

    def execute_role_swaps(self, segments_to_move: List[Segment], direction: RoleSwapDirection):
        """Execute multiple role swaps in single gprecoverseg -r call"""

        assert (len(segments_to_move) > 0)

        segids = [segment.getSegmentContentId() for segment in segments_to_move]
        dbids = [segment.getSegmentDbId() for segment in segments_to_move]

        seg_list = ', '.join(str(seg) for seg in segids)
        dbid_list = ', '.join(str(dbid) for dbid in dbids)

        dbconn.execSQL(self.conn, "BEGIN")

        # check the current status of 'preferred_role' and 'role' for all requested dbids
        # in order to recover properly from the previous interrupted run (if any)

        cnt_preferred_role_p = \
            int(dbconn.queryRow(self.conn,
                f"SELECT COUNT(1) FROM gp_segment_configuration WHERE preferred_role = 'p' AND dbid IN ({dbid_list})")[0])
        cnt_role_p = \
            int(dbconn.queryRow(self.conn,
                f"SELECT COUNT(1) FROM gp_segment_configuration WHERE role = 'p' AND dbid IN ({dbid_list})")[0])
        cnt_preferred_role_m = \
            int(dbconn.queryRow(self.conn,
                f"SELECT COUNT(1) FROM gp_segment_configuration WHERE preferred_role = 'm' AND dbid IN ({dbid_list})")[0])
        cnt_role_m = \
            int(dbconn.queryRow(self.conn,
                f"SELECT COUNT(1) FROM gp_segment_configuration WHERE role = 'm' AND dbid IN ({dbid_list})")[0])

        # if some have 'preferred_role'='p' and some have 'preferred_role'='m' - shouldn't happen, error out, needs to be resolved manually.
        # also some sanity check that there are no other values in catalog except 'm' and 'p' for 'preferred_role'.
        if ((cnt_preferred_role_p > 0 and cnt_preferred_role_m > 0) or
             (cnt_preferred_role_p + cnt_preferred_role_m != len(segids))):
            raise Exception("Error in catalog configuration: "
                            f"for dbid list ({dbid_list}) "
                            f"{cnt_preferred_role_p} have 'p' preferred role, and "
                            f"{cnt_preferred_role_m} have 'm' preferred role")

        is_catalog_update_required = False
        is_gprecoverseg_required = False

        if direction == self.RoleSwapDirection.PRIMARY_TO_MIRROR:
            # if all have 'preferred_role'='p' - it is our first run, need to update catalog and launch gprecoverseg
            if cnt_preferred_role_p == len(segids):
                is_catalog_update_required = True
                is_gprecoverseg_required = True
            else:
                # if all have 'preferred_role'='m' and not all have 'role'='m' - previous gprecoverseg was interrupted, need to launch it again
                if cnt_role_m != cnt_preferred_role_m:
                    is_gprecoverseg_required = True
                # if all have 'preferred_role'='m' and 'role'='m' - we've done everything on previous interrupted run, nothing to do
        else:
            # moving back in MIRROR_TO_PRIMARY direction
            # if all have 'preferred_role'='m' - it is our first run, need to update catalog and launch gprecoverseg
            if cnt_preferred_role_m == len(segids):
                is_catalog_update_required = True
                is_gprecoverseg_required = True
            else:
                # if all have 'preferred_role'='p' and not all have 'role'='p' - previous gprecoverseg was interrupted, need to launch it again
                if cnt_role_p != cnt_preferred_role_p:
                    is_gprecoverseg_required = True
                # if all have 'preferred_role'='p' and 'role'='p' - we've done everything on previous interrupted run, nothing to do

        if is_catalog_update_required:
            dbconn.execSQL(self.conn, "UPDATE gp_segment_configuration SET preferred_role = 't' WHERE "
                           f"content IN ({seg_list}) AND preferred_role = 'm'")
            dbconn.execSQL(self.conn, "UPDATE gp_segment_configuration SET preferred_role = 'm' WHERE "
                           f"content IN ({seg_list}) AND preferred_role = 'p'")
            dbconn.execSQL(self.conn, "UPDATE gp_segment_configuration SET preferred_role = 'p' WHERE "
                           f"content IN ({seg_list}) AND preferred_role = 't'")

        dbconn.execSQL(self.conn, "COMMIT")

        if direction == self.RoleSwapDirection.PRIMARY_TO_MIRROR:
            inject_fault('FAULT_BEFORE_GPRECOVERSEG_PRIMARY_TO_MIRROR')
        else:
            inject_fault('FAULT_BEFORE_GPRECOVERSEG_MIRROR_TO_PRIMARY')

        if is_gprecoverseg_required:
            recoverseg_options = "-r -a"

            if self.options.parallel is not None:
                batch_size = self.options.parallel
                # gprecoverseg has its own limitation for batch size,
                # need to consider it here.
                if batch_size > MAX_COORDINATOR_NUM_WORKERS:
                    batch_size = MAX_COORDINATOR_NUM_WORKERS
                recoverseg_options += f' -B {batch_size}'
                if self.options.replay_lag is not None:
                    recoverseg_options = recoverseg_options + f' --replay-lag {self.options.replay_lag}'
                if self.options.logfile_directory is not None:
                    recoverseg_options = recoverseg_options + f' -l "{str(self.options.logfile_directory)}"'
            try:
                self.cmd = GpRecoverSeg("Running gprecoverseg", options=recoverseg_options)
                self.cmd.run(validateAfter=True)
            except Exception as e:
                logger.error(str(e))
                error_msg = f"Failed to execute 'gprecoverseg {recoverseg_options}'"
                raise Exception(error_msg)
            finally:
                self.cmd = None

    def lookup_seg(self, gparray: gparray.GpArray, seg: Segment) -> bool:
        """ Look up the segment gpdb by address, port, and dataDirectory """
        for db in gparray.getDbList():
            if (seg.getSegmentHostName() == db.getSegmentHostName() and
                seg.getSegmentPort() == db.getSegmentPort() and
                seg.getSegmentDataDirectory() == db.getSegmentDataDirectory()):
                return True
        return False

    def create_config_file(self, steps: List[RebalanceStepMoveMirror]) -> str:
        filename = f'/tmp/ggrebalance_move_config_pid{os.getpid()}'
        gparray = GpArray.initFromCatalog(self.dburl, utility=True)
        with open(filename, 'w') as fp:
            for step in steps:
                assert isinstance(step, RebalanceStepMoveMirror)
                move = step.getMove()
                segment_current_info = move.seg
                # We need to check if the original segment location exists in gp_segment_configuration.
                # If not, it means that we've already tried to move this segment but failed after the catalog update,
                # and now we need to use the dst address as the old address (as it is already in the catalog, and
                # gpmovemirrors will do validation against it).
                if self.lookup_seg(gparray, segment_current_info):
                    cfg_line = f'{segment_current_info.getSegmentHostName()}|{segment_current_info.getSegmentPort()}|{segment_current_info.getSegmentDataDirectory()} '
                else:
                    cfg_line = f'{move.dstHost.hostname}|{move.target_port}|{move.target_datadir} '
                # If we perform a rollback, we use the original segment location as the target address.
                if step.isRollback():
                    cfg_line += f'{segment_current_info.getSegmentHostName()}|{segment_current_info.getSegmentPort()}|{segment_current_info.getSegmentDataDirectory()}\n'
                else:
                    cfg_line += f'{move.dstHost.hostname}|{move.target_port}|{move.target_datadir}\n'
                fp.write(cfg_line)
        return filename

    def shutdown(self) -> None:
        self.shutdown_requested = True
        if self.cmd != None:
            self.cmd.cancel()

    def state_is_final(self, state: str) -> bool:
        return state == self.states_main_rebalance_flow[-1]

    def get_state_after_interrupt(self, prev_state) -> str:
        if prev_state in self.states_rollback_rebalance_flow[:-1]:
            prev_idx = self.states_rollback_rebalance_flow.index(prev_state)
            return self.states_rollback_rebalance_flow[prev_idx + 1]

        if (prev_state in ['STATE_REBALANCE_ROLLBACK_PREPARE_MOVES_DONE',
                           'STATE_REBALANCE_EXECUTION_STARTED',
                           'STATE_REBALANCE_MOVES_SUCCEEDED',
                           'STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_DONE']):
            return 'STATE_REBALANCE_EXECUTION_STARTED'

        prev_idx = self.states_main_rebalance_flow.index(prev_state)

        return self.states_main_rebalance_flow[prev_idx + 1]

    def reset_in_progress_execution_steps(self) -> None:
        in_progress_steps = self.rebalance_schema.getExecutionSteps([RebalanceStep.Status.IN_PROGRESS])
        for step in in_progress_steps:
            step.setStatus(RebalanceStep.Status.ERROR, step.isRollback())
            self.rebalance_schema.updateExecutionStep(step)

    def process_error_execution_steps(self) -> None:
        dbconn.execSQL(self.conn, "BEGIN")
        try:
            error_steps = self.rebalance_schema.getExecutionSteps([RebalanceStep.Status.ERROR])
            if len(error_steps) == 0:
                return

            # All steps in an errored batch should be the same type, so we probe only
            # the first one to detect the type and process accordingly.
            if isinstance(error_steps[0], RebalanceStepMoveMirror):
                self.process_error_execution_steps_mirror_moves(error_steps)
            else:
                self.process_error_execution_steps_switchovers(error_steps)
        finally:
            dbconn.execSQL(self.conn, "COMMIT")

    def process_error_execution_steps_mirror_moves(self, error_steps: List[RebalanceStep]) -> None:
        self.logger.info('Process failed segment moves...')
        steps_left_todo = self.rebalance_schema.getExecutionSteps([RebalanceStep.Status.PLANNED, RebalanceStep.Status.APPROVE_REQUIRED])
        for step in error_steps:
            self.logger.info(f'Checking error status for step: {str(step)}')
            dbid = step.getMove().seg.getSegmentDbId()
            target_hostname = step.getMove().dstHost.hostname
            target_datadir = step.getMove().target_datadir
            target_port = step.getMove().target_port

            catalog_segment_info = self.get_catalog_gp_segment_configuration_for_dbid(dbid)
            self.logger.info(f'Segment info from catalog: {str(catalog_segment_info)}')

            gp_segment_configuration_updated = (catalog_segment_info.hostname == target_hostname)

            port_updated = False
            if gp_segment_configuration_updated:
                port_updated = (self.get_postgresql_conf_port(target_hostname, target_datadir) == target_port)

            self.logger.info(f'gp_segment_configuration is updated: {gp_segment_configuration_updated}')
            self.logger.info(f'Port is updated: {port_updated}')

            time_waited = 0
            SLEEP_PERIOD_SEC = 1.0
            TIMEOUT_SEC = 120.0
            if port_updated:
                self.logger.info(f'Start checking if segment is up with timeout of {TIMEOUT_SEC} sec.')
                while time_waited < TIMEOUT_SEC:
                    # Start polling segment status with timeout
                    segment_process_started = SegmentStatus.remote('Segment status check', target_hostname, target_datadir).was_successful()
                    if segment_process_started:
                        catalog_segment_info = self.get_catalog_gp_segment_configuration_for_dbid(dbid)
                        if catalog_segment_info.isSegmentUp() and catalog_segment_info.isSegmentModeSynchronized():
                            self.logger.info('The step is complete, mark it as done')
                            step.setStatus(RebalanceStep.Status.DONE, step.isRollback())
                            self.rebalance_schema.updateExecutionStep(step)
                            break

                    time.sleep(SLEEP_PERIOD_SEC)
                    time_waited = time_waited + SLEEP_PERIOD_SEC
                    if time_waited >= TIMEOUT_SEC and interactive_check_yesno(self.options.interactive, None,
                                                                              'Timeout waiting for segment start, wait again?', default = 'N'):
                        time_waited = 0

            # Continue with the next step, if we already marked this one
            if step.getStatus() == RebalanceStep.Status.DONE:
                continue

            allow_retry = step.isRetryAllowed()
            if not allow_retry:
                self.logger.warning("We've run out of retry attempts")

            if allow_retry and interactive_check_yesno(self.options.interactive, None, 'Retry step?', default = 'Y'):
                if step.isRollback():
                    self.logger.info('Plan to retry rollback step')
                    step.setStatus(RebalanceStep.Status.PLANNED, True)
                else:
                    self.logger.info('Plan to retry step')
                    step.setStatus(RebalanceStep.Status.PLANNED)
                self.rebalance_schema.updateExecutionStep(step)
                continue

            if not step.isRollback() and interactive_check_yesno(self.options.interactive, None, 'Rollback step?', default = 'Y'):
                self.logger.info('Plan to rollback step')
                step.setStatus(RebalanceStep.Status.PLANNED, True)
            else:
                self.logger.info('Cancel step')
                step.setStatus(RebalanceStep.Status.CANCELLED)
            self.rebalance_schema.updateExecutionStep(step)

        # Mark dependent steps accordingly
        self.mark_dependent_steps_on_error(error_steps, steps_left_todo)

    def process_error_execution_steps_switchovers(self, error_steps: List[RebalanceStep]) -> None:
        self.logger.info('Process failed switchovers...')
        steps_left_todo = self.rebalance_schema.getExecutionSteps([RebalanceStep.Status.PLANNED, RebalanceStep.Status.APPROVE_REQUIRED])
        for step in error_steps:
            self.logger.info(f'Processing error status for switchover step: {str(step)}')
            if interactive_check_yesno(self.options.interactive, None, 'Retry step?', default = 'Y'):
                if step.isRollback():
                    self.logger.info('Plan to retry rollback step')
                    step.setStatus(RebalanceStep.Status.PLANNED, True)
                else:
                    self.logger.info('Plan to retry step')
                    step.setStatus(RebalanceStep.Status.PLANNED)
                self.rebalance_schema.updateExecutionStep(step)
                continue

            if not step.isRollback() and interactive_check_yesno(self.options.interactive, None, 'Rollback step?', default = 'Y'):
                self.logger.info('Plan to rollback step')
                step.setStatus(RebalanceStep.Status.PLANNED, True)

                # Revert type of switchover
                rollback_step_for_switchover = None
                if isinstance(step, RebalanceStepSwitchoverToMirror):
                    rollback_step_for_switchover = RebalanceStepSwitchoverToPrimary(step.getMove())
                elif isinstance(step, RebalanceStepSwitchoverToPrimary):
                    rollback_step_for_switchover = RebalanceStepSwitchoverToMirror(step.getMove())

                if rollback_step_for_switchover:
                    rollback_step_for_switchover.setMoveOrder(step.getMoveOrder())
                    rollback_step_for_switchover.setStatus(step.getStatus(), True)
                    step = rollback_step_for_switchover
            else:
                self.logger.info('Cancel step')
                step.setStatus(RebalanceStep.Status.CANCELLED)
            self.rebalance_schema.updateExecutionStep(step)

        # Mark dependent steps accordingly
        self.mark_dependent_steps_on_error(error_steps, steps_left_todo)

    def mark_dependent_steps_on_error(self, error_steps: List[RebalanceStep], steps_left_todo: List[RebalanceStep]) -> None:
        # 1. If there are steps planned for ROLLBACK - we mark all left todo steps for the same content as already rolled back
        if not self.is_rollback_flow:
            for step in error_steps:
                if step.getStatus() == RebalanceStep.Status.PLANNED and step.isRollback():
                    content_id = step.getMove().seg.getSegmentContentId()
                    for step_todo in steps_left_todo:
                        if step_todo.getMove().seg.getSegmentContentId() == content_id:
                            self.logger.info(f'Mark as already rolled back the dependent step {step_todo}')
                            step_todo.setStatus(RebalanceStep.Status.DONE, True)
                            self.rebalance_schema.updateExecutionStep(step_todo)
        # 2. If there are any cancelled steps - we need to:
        #  a. cancel all not yet done steps of the same dbid,
        #  b. and *ALL* switchovers,
        #  c. and do cancelation recursively.
        # But, actually, it means that we need to cancel everything besides steps revived from the ERROR state just above,
        # as left todo steps didn't get into this ERRORed batch, meaning they must have different step type (meaning switchover).
        if any(step.getStatus() == RebalanceStep.Status.CANCELLED for step in error_steps):
            for step_todo in steps_left_todo:
                self.logger.info(f'Mark as CANCELLED the step {step_todo}')
                step_todo.setStatus(RebalanceStep.Status.CANCELLED, step_todo.isRollback())
                self.rebalance_schema.updateExecutionStep(step_todo)

    def get_catalog_gp_segment_configuration_for_dbid(self, dbid: int) -> Segment:
        row = dbconn.queryRow(self.conn,
            f"SELECT dbid||'|'||content||'|'||role||'|'||preferred_role||'|'||mode||'|'||status||'|'||hostname||'|'||address||'|'||port||'|'||datadir "
            f"FROM gp_segment_configuration WHERE dbid = {dbid}")
        return Segment.initFromString(row[0])

    def get_postgresql_conf_port(self, hostname: str, datadir: str) -> int:
        cmd = gp.GpConfigHelper(f'get port parameter on host {hostname}',
                                datadir,
                                'port',
                                getParameter=True,
                                ctxt=gp.REMOTE,
                                remoteHost=hostname)
        cmd.run()

        if not cmd.was_successful():
            self.logger.info(f"Failed to get port from postgresql.conf on {hostname}: {cmd.get_stderr()}")
            return -1

        output = cmd.get_value()
        output = output if '#' not in output else output[0:output.find('#')]
        output = output.strip()
        if not output or not output.isdigit():
            return -1

        return int(output)

    @staticmethod
    def convert_moves_to_rebalance_steps(moves: List[LogicalMove]) -> List[RebalanceStep]:
        # In the loop below we create a list of rebalance execution steps from the plan's list of moves.
        # Mirror's movemenet is simply translated into single step.
        # But primary's movement is translated into 3 steps:
        #   1st is switchover with the mirror;
        #   2nd is movement itself;
        #   3rd is the back switchover.
        # If there are several consequent primary movements, we assume that they can be done in parallel,
        # and, to allow batch processing for them, we re-order their steps to combine together
        # the respective switchover and movement steps.
        rebalance_steps = []

        batch_mirror_steps = []
        batch_primary_steps = [[],[],[]]

        def fill_rebalance_steps():
            nonlocal rebalance_steps
            nonlocal batch_mirror_steps
            nonlocal batch_primary_steps
            assert not (len(batch_mirror_steps) > 0 and len(batch_primary_steps[0]) > 0)
            if len(batch_mirror_steps) > 0:
                rebalance_steps = rebalance_steps + batch_mirror_steps
            if len(batch_primary_steps[0]) > 0:
                flattened_batch_primary_steps = [step for sublist in batch_primary_steps for step in sublist]
                rebalance_steps = rebalance_steps + flattened_batch_primary_steps
            # clear the batches
            batch_mirror_steps = []
            batch_primary_steps = [[],[],[]]

        prev_move = None
        for move in moves:
            # if the move type switched, fill rebalance_steps with the content of
            # previously gathered batches
            if prev_move != None and prev_move.seg.getSegmentRole() != move.seg.getSegmentRole():
                fill_rebalance_steps()
            prev_move = move
            # add steps to the current batch
            if move.seg.isSegmentMirror():
                batch_mirror_steps.append(RebalanceStepMoveMirror(move))
            else:
                batch_primary_steps[0].append(RebalanceStepSwitchoverToMirror(move))
                batch_primary_steps[1].append(RebalanceStepMoveMirror(move))
                batch_primary_steps[2].append(RebalanceStepSwitchoverToPrimary(move))
        fill_rebalance_steps() # fill what's left

        return rebalance_steps

    # state callbacks start here

    @wrap_func_with_faults
    def on_enter_STATE_CHECK_PREVIOUS_RUN(self) -> None:
        state_from_prev_run = self.rebalance_schema.getRebalanceStateFromPreviousRun()
        self.is_rollback_flow = self.rebalance_schema.isRollbackRebalanceFlow(self.states_rollback_rebalance_flow[0])


        if state_from_prev_run == STATE_NOT_DEFINED:
            self.trigger('move_to_STATE_REBALANCE_STARTED')
        elif self.state_is_final(state_from_prev_run):
            self.logger.info('Cluster is already rebalanced...')
        else:
            if self.is_rollback_flow:
                self.logger.info('Continue interrupted rebalance rollback operation...')
            else:
                self.logger.info('Continue interrupted rebalance operation...')
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
    def on_enter_STATE_REBALANCE_STARTED(self) -> None:
        self.trigger('move_to_STATE_REBALANCE_PREPARE_MOVES_STARTED')

    @wrap_func_with_faults
    def on_enter_STATE_REBALANCE_PREPARE_MOVES_STARTED(self) -> None:
        if not self.rebalance_plan.getMoves():
            raise Exception('Rebalance executor was launched with a plan without segment movements')

        rebalance_steps = self.convert_moves_to_rebalance_steps(self.rebalance_plan.getMoves())

        self.logger.info('Saving following rebalance execution steps:')

        move_order = 0
        for step in rebalance_steps:
            step.setMoveOrder(move_order)
            move_order += 1
            self.logger.info(str(step))

        self.rebalance_schema.saveExecutionSteps(rebalance_steps)

        self.logger.info('Saved rebalance execution steps')

        self.trigger('move_to_STATE_REBALANCE_PREPARE_MOVES_DONE')

    @wrap_func_with_faults
    def on_enter_STATE_REBALANCE_PREPARE_MOVES_DONE(self) -> None:
        self.trigger('move_to_STATE_REBALANCE_EXECUTION_STARTED')

    @wrap_func_with_faults
    def on_enter_STATE_REBALANCE_EXECUTION_STARTED(self) -> None:
        if self.rebalance_schema.allExecutionStepsAreDone():
            self.trigger('move_to_STATE_REBALANCE_EXECUTION_DONE')
            return

        # In normal execution we shouldn't have IN_PROGRESS steps at this moment.
        # If they are presented, it means they are left from previous interrupted run.
        # Set them to ERROR state.
        self.reset_in_progress_execution_steps()

        self.process_error_execution_steps()

        steps_to_execute = self.rebalance_schema.getExecutionSteps([RebalanceStep.Status.PLANNED, RebalanceStep.Status.APPROVE_REQUIRED])

        if len(steps_to_execute) > 0:

            if steps_to_execute[0].getStatus() == RebalanceStep.Status.APPROVE_REQUIRED:
                self.trigger('move_to_STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_STARTED')
                return

            current_batch = []
            for step in steps_to_execute:
                # fill current batch until we found a step that requires approve
                # (it will be handled the next time we enter this state),
                if (step.getStatus() == RebalanceStep.Status.APPROVE_REQUIRED or
                    # or till the type of RebalanceStep changes.
                    (len(current_batch) > 0 and (type(current_batch[0]) is not type(step)))):
                    break

                step.setStatus(RebalanceStep.Status.IN_PROGRESS, step.isRollback())
                self.rebalance_schema.updateExecutionStep(step)
                current_batch.append(step)

            if isinstance(current_batch[0], RebalanceStepMoveMirror):
                self.logger.info('Rebalance - start moving segments:')
                for step in current_batch:
                    self.logger.info(str(step))
                self.process_moves(current_batch)
                self.logger.info('Rebalance - end moving segments')
            else:
                direction = self.RoleSwapDirection.PRIMARY_TO_MIRROR
                if not isinstance(current_batch[0], RebalanceStepSwitchoverToMirror):
                    direction = self.RoleSwapDirection.MIRROR_TO_PRIMARY
                segments = [step.getMove().seg for step in current_batch]
                self.logger.info(f'Rebalance - start role swap {str(direction)} for segments:')
                for segment in segments:
                    self.logger.info(str(segment))
                self.execute_role_swaps(segments, direction)
                self.logger.info('Rebalance - end role swap')

            for step in steps_to_execute:
                if step.getStatus() == RebalanceStep.Status.IN_PROGRESS:
                    step.setStatus(RebalanceStep.Status.DONE, step.isRollback())
                    self.rebalance_schema.updateExecutionStep(step)

        self.trigger('move_to_STATE_REBALANCE_MOVES_SUCCEEDED')

    @wrap_func_with_faults
    def on_enter_STATE_REBALANCE_MOVES_SUCCEEDED(self) -> None:
        self.trigger('move_to_STATE_REBALANCE_EXECUTION_STARTED')

    @wrap_func_with_faults
    def on_enter_STATE_REBALANCE_EXECUTION_DONE(self) -> None:
        self.trigger('move_to_STATE_REBALANCE_DONE')

    @wrap_func_with_faults
    def on_enter_STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_STARTED(self) -> None:

        # Approve all consequent steps that require approval
        steps = self.rebalance_schema.getExecutionSteps([RebalanceStep.Status.PLANNED, RebalanceStep.Status.APPROVE_REQUIRED])

        assert len(steps) > 0

        steps_to_approve = []
        for step in steps:
            if step.getStatus() != RebalanceStep.Status.APPROVE_REQUIRED:
                break
            steps_to_approve.append(step)

        msg = 'Following switchovers require approval:\n'
        for step in steps_to_approve:
            msg += str(step)
            msg += '\n'
        if not interactive_check_yesno(self.options.interactive, msg, 'Approve switchovers?', default = 'Y'):
            raise Exception('Switchovers were not approved, interrupting execution')

        for step in steps_to_approve:
            step.setStatus(RebalanceStep.Status.PLANNED, step.isRollback())
            self.rebalance_schema.updateExecutionStep(step)

        self.trigger('move_to_STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_DONE')

    @wrap_func_with_faults
    def on_enter_STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_DONE(self) -> None:
        self.trigger('move_to_STATE_REBALANCE_EXECUTION_STARTED')

    @wrap_func_with_faults
    def on_enter_STATE_REBALANCE_ROLLBACK_STARTED(self) -> None:
        self.is_rollback_flow = True
        self.logger.info('Starting rebalance rollback')
        self.trigger('move_to_STATE_REBALANCE_ROLLBACK_PREPARE_MOVES_STARTED')

    @wrap_func_with_faults
    def on_enter_STATE_REBALANCE_ROLLBACK_PREPARE_MOVES_STARTED(self) -> None:

        self.logger.info('Start preparing steps for rollback...')
        actual_rollback_steps_cnt = 0
        rollback_steps = self.rebalance_schema.getExecutionSteps([])

        if len(rollback_steps) > 0:
            move_order = rollback_steps[-1].getMoveOrder()

            for i, step in enumerate(rollback_steps):
                # reverse the move order
                step.setMoveOrder(move_order)
                move_order -= 1
                if step.isRollback():
                    continue

                # convert not yet executed steps as already rolled back
                if step.getStatus() in [RebalanceStep.Status.APPROVE_REQUIRED, RebalanceStep.Status.PLANNED]:
                    step.setStatus(RebalanceStep.Status.DONE, True)
                    actual_rollback_steps_cnt += 1
                elif step.getStatus() in [RebalanceStep.Status.IN_PROGRESS, RebalanceStep.Status.DONE, RebalanceStep.Status.ERROR]:
                    step.setStatus(RebalanceStep.Status.PLANNED, True)
                    actual_rollback_steps_cnt += 1
                elif step.getStatus() == RebalanceStep.Status.CANCELLED:
                    # We do nothing for CANCELLED steps - for now they can be processed only if run ggrebalance from scratch.
                    self.logger.warning(f'Step {str(step)} is marked as CANCELLED, and skipped during ROLLBACK processing.')
                    continue

                rollback_step_for_switchover = None

                # Revert type of switchover
                if isinstance(step, RebalanceStepSwitchoverToMirror):
                    rollback_step_for_switchover = RebalanceStepSwitchoverToPrimary(step.getMove())
                elif isinstance(step, RebalanceStepSwitchoverToPrimary):
                    rollback_step_for_switchover = RebalanceStepSwitchoverToMirror(step.getMove())

                if rollback_step_for_switchover:
                    rollback_step_for_switchover.setMoveOrder(step.getMoveOrder())
                    rollback_step_for_switchover.setStatus(step.getStatus(), True)
                    if step.getStatus() == RebalanceStep.Status.PLANNED:
                        rollback_step_for_switchover.setStatus(RebalanceStep.Status.APPROVE_REQUIRED, True)
                    rollback_steps[i] = rollback_step_for_switchover

            rollback_steps.sort(key=lambda x: x.getMoveOrder())

        if actual_rollback_steps_cnt > 0:
            self.logger.info('Saving following rollback rebalance execution steps:')
            for step in rollback_steps:
                self.logger.info(str(step))
            self.rebalance_schema.saveExecutionSteps(rollback_steps)
            self.logger.info('Saved rollback rebalance execution steps')
        else:
            self.logger.info('No steps to rollback found for rebalance')

        self.trigger('move_to_STATE_REBALANCE_ROLLBACK_PREPARE_MOVES_DONE')

    @wrap_func_with_faults
    def on_enter_STATE_REBALANCE_ROLLBACK_PREPARE_MOVES_DONE(self) -> None:
        self.trigger('move_to_STATE_REBALANCE_EXECUTION_STARTED')

    @wrap_func_with_faults
    def on_enter_STATE_REBALANCE_DONE(self) -> None:
        if self.is_rollback_flow:
            self.rebalance_schema.dropSchema()
            self.logger.info('Rebalance rollback is complete')
        else:
            self.logger.info('Rebalance is complete')

    @wrap_func_with_faults
    def on_enter_STATE_ERROR(self) -> None:
        raise Exception('Rebalance execution entered STATE_ERROR')

    # state callbacks end here
