#!/usr/bin/env python3

from transitions import Machine
from enum import Enum

try:
    from gppylib.commands.unix import *
    from gppylib.commands.gp import *
    from gppylib.gplog import *
    from gppylib.commands.gp import GpMoveMirrors
    from gppylib.system.environment import *
    from gprebalance_modules.planner import *
    from gprebalance_modules.rebalance_schema import RebalanceSchema, STATE_NOT_DEFINED
    from gprebalance_modules.rebalance_step import *
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
            'source': ['STATE_REBALANCE_PREPARE_MOVES_DONE', 'STATE_REBALANCE_MOVES_SUCCEEDED', 'STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_DONE'],
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

    def __init__(self, conn: dbconn.Connection, schema: RebalanceSchema, logger: Any, options: Any, gpArray: gparray.GpArray):
        self.logger = logger
        self.options = options
        self.shutdown_requested = False
        self.gparray = gpArray
        self.conn = conn
        self.rebalance_schema = schema
        self.cmd = None

        self.machine = Machine(model = self,
                               queued=True,
                               states = self.states_main_rebalance_flow + self.states_not_logged,
                               transitions = self.transitions,
                               initial = 'STATE_REBALANCE_INIT',
                               before_state_change = 'on_every_state')

    def on_every_state(self) -> None:
        if self.shutdown_requested:
            self.logger.info('Rebalance was interrupted')
            raise Exception('Rebalance was interrupted')

        if self.state in self.states_main_rebalance_flow:
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

    def process_moves(self, moves: List[LogicalMove]):
        if len(moves) == 0:
            return

        filename = self.create_config_file(moves)
        gpmovemirrors_options = f'-a -i {filename}'

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

    def lookup_seg(self, seg: Segment) -> bool:
        """ Look up the segment gpdb by address, port, and dataDirectory """
        for db in self.gparray.getDbList():
            if (seg.getSegmentHostName() == db.getSegmentHostName() and
                seg.getSegmentPort() == db.getSegmentPort() and
                seg.getSegmentDataDirectory() == db.getSegmentDataDirectory()):
                return True
        return False

    def create_config_file(self, moves: List[LogicalMove]) -> str:
        filename = f'/tmp/ggrebalance_move_config_pid{os.getpid()}'
        with open(filename, 'w') as fp:
            for move in moves:
                segment_current_info = move.seg
                if not self.lookup_seg(segment_current_info):
                    self.logger.info(f'Skip segment for gpmovemirrors: {str(segment_current_info)}')
                    continue
                cfg_line = f'{segment_current_info.getSegmentHostName()}|{segment_current_info.getSegmentPort()}|{segment_current_info.getSegmentDataDirectory()} '
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
        if (prev_state == 'STATE_REBALANCE_EXECUTION_STARTED' or
            prev_state == 'STATE_REBALANCE_MOVES_SUCCEEDED' or
            prev_state == 'STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_DONE'):
            return 'STATE_REBALANCE_EXECUTION_STARTED'

        prev_idx = self.states_main_rebalance_flow.index(prev_state)

        return self.states_main_rebalance_flow[prev_idx + 1]

    def reset_in_progress_execution_steps(self) -> None:
        in_progress_steps = self.rebalance_schema.getExecutionSteps([RebalanceStep.Status.IN_PROGRESS])
        for step in in_progress_steps:
            step.setStatus(RebalanceStep.Status.PLANNED)
            self.rebalance_schema.updateExecutionStep(step)

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

    @wrap_state_func_with_faults
    def on_enter_STATE_CHECK_PREVIOUS_RUN(self) -> None:
        state_from_prev_run = self.rebalance_schema.getRebalanceStateFromPreviousRun()

        if state_from_prev_run == STATE_NOT_DEFINED:
            self.trigger('move_to_STATE_REBALANCE_STARTED')
        elif self.state_is_final(state_from_prev_run):
            self.logger.info('Cluster is already rebalanced...')
        else:
            self.logger.info('Continue interrupted rebalance operation...')
            self.logger.info(f"Previous run stopped after state '{state_from_prev_run}', trying to continue from the next state...")
            try:
                next_state = self.get_state_after_interrupt(state_from_prev_run)
            except:
                self.logger.error("Can't determine next state. Try to execute cleanup.")
                self.trigger('move_to_STATE_ERROR')
                return
            # use auto to_«state» method to recover
            self.trigger(f'to_{next_state}')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_STARTED(self) -> None:
        self.trigger('move_to_STATE_REBALANCE_PREPARE_MOVES_STARTED')

    @wrap_state_func_with_faults
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

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_PREPARE_MOVES_DONE(self) -> None:
        self.trigger('move_to_STATE_REBALANCE_EXECUTION_STARTED')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_EXECUTION_STARTED(self) -> None:

        if self.rebalance_schema.allExecutionStepsAreDone():
            self.trigger('move_to_STATE_REBALANCE_EXECUTION_DONE')
            return

        # In normal execution we shouldn't have IN_PROGRESS steps at this moment.
        # If they are presented, it means they are left from previous interrupted run.
        # Bring them back to PLANNED state, so we can try to process them again.
        self.reset_in_progress_execution_steps()

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

                step.setStatus(RebalanceStep.Status.IN_PROGRESS)
                self.rebalance_schema.updateExecutionStep(step)
                current_batch.append(step)

            if isinstance(current_batch[0], RebalanceStepMoveMirror):
                self.logger.info('Rebalance - start moving segments:')
                moves = [step.getMove() for step in current_batch]
                for move in moves:
                    self.logger.info(str(move))
                self.process_moves(moves)
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

            # TODO: check the errored segments here, once we implement rollback for the rebalance.
            # For now if some error happened, the entire tool will halt its work, so if we reached this point
            # just mark all steps as done.
            for step in steps_to_execute:
                if step.getStatus() == RebalanceStep.Status.IN_PROGRESS:
                    step.setStatus(RebalanceStep.Status.DONE)
                    self.rebalance_schema.updateExecutionStep(step)

        self.trigger('move_to_STATE_REBALANCE_MOVES_SUCCEEDED')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_MOVES_SUCCEEDED(self) -> None:
        self.trigger('move_to_STATE_REBALANCE_EXECUTION_STARTED')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_EXECUTION_DONE(self) -> None:
        self.trigger('move_to_STATE_REBALANCE_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_STARTED(self) -> None:

        # Approve all consequent steps that require approval
        steps = self.rebalance_schema.getExecutionSteps([RebalanceStep.Status.PLANNED, RebalanceStep.Status.APPROVE_REQUIRED])

        assert len(steps) > 0

        for step in steps:
            if step.getStatus() != RebalanceStep.Status.APPROVE_REQUIRED:
                break
            # TODO: we'll need to add logic here to get approval from the user in the interactive mode,
            # once we start implementing the interactive mode.
            # In non-interactive mode we assume that the switchover is always approved.
            step.setStatus(RebalanceStep.Status.PLANNED)
            self.rebalance_schema.updateExecutionStep(step)

        self.trigger('move_to_STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_EXECUTION_AWAITING_SWITCHOVER_APPROVE_DONE(self) -> None:
        self.trigger('move_to_STATE_REBALANCE_EXECUTION_STARTED')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_DONE(self) -> None:
        pass

    @wrap_state_func_with_faults
    def on_enter_STATE_ERROR(self) -> None:
        raise Exception('Rebalance execution entered STATE_ERROR')

    # state callbacks end here
