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
    from gppylib.fault_injection import *
except ImportError as e:
    sys.exit('ERROR: Cannot import modules.  Please check that you have sourced greenplum_path.sh.  Detail: ' + str(e))

class RebalanceSM:

    states_not_logged = [
        'STATE_REBALANCE_INIT',
        'STATE_CHECK_PREVIOUS_RUN'
    ]

    states_main_rebalance_flow = [
        'STATE_REBALANCE_STARTED',
        'STATE_REBALANCE_MOVE_MIRRORS_STARTED',
        'STATE_REBALANCE_MOVE_MIRRORS_DONE',
        'STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_STARTED',
        'STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_DONE',
        'STATE_REBALANCE_MOVE_PRIMARIES_STARTED',
        'STATE_REBALANCE_MOVE_PRIMARIES_DONE',
        'STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_STARTED',
        'STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_DONE',
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
            'trigger': 'move_to_STATE_REBALANCE_MOVE_MIRRORS_STARTED',
            'source': 'STATE_REBALANCE_STARTED',
            'dest': 'STATE_REBALANCE_MOVE_MIRRORS_STARTED'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_MOVE_MIRRORS_DONE',
            'source': 'STATE_REBALANCE_MOVE_MIRRORS_STARTED',
            'dest': 'STATE_REBALANCE_MOVE_MIRRORS_DONE'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_STARTED',
            'source': 'STATE_REBALANCE_MOVE_MIRRORS_DONE',
            'dest': 'STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_STARTED'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_DONE',
            'source': 'STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_STARTED',
            'dest': 'STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_DONE'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_MOVE_PRIMARIES_STARTED',
            'source': 'STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_DONE',
            'dest': 'STATE_REBALANCE_MOVE_PRIMARIES_STARTED'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_MOVE_PRIMARIES_DONE',
            'source': 'STATE_REBALANCE_MOVE_PRIMARIES_STARTED',
            'dest': 'STATE_REBALANCE_MOVE_PRIMARIES_DONE'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_STARTED',
            'source': 'STATE_REBALANCE_MOVE_PRIMARIES_DONE',
            'dest': 'STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_STARTED'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_DONE',
            'source': 'STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_STARTED',
            'dest': 'STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_DONE'
        },
        {
            'trigger': 'move_to_STATE_REBALANCE_DONE',
            'source': ['STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_DONE', 'STATE_REBALANCE_MOVE_MIRRORS_DONE'],
            'dest': 'STATE_REBALANCE_DONE'
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
        filename = self.create_config_file(moves)
        gpmovemirrors_options = f'-a -i {filename}'

        if self.options.batch_size is not None:
            batch_size = self.options.batch_size
            # gpmovemirrors has its own limitation for batch size,
            # need to consider it here.
            if batch_size > MAX_COORDINATOR_NUM_WORKERS:
                batch_size = MAX_COORDINATOR_NUM_WORKERS
            gpmovemirrors_options += f' -B {batch_size}'

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
        prev_idx = self.states_main_rebalance_flow.index(prev_state)
        return self.states_main_rebalance_flow[prev_idx + 1]

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
        self.trigger('move_to_STATE_REBALANCE_MOVE_MIRRORS_STARTED')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_MOVE_MIRRORS_STARTED(self) -> None:
        self.logger.info('Rebalance - start moving mirrors')
        self.process_moves(self.moves_mirrors)
        self.logger.info('Rebalance - end moving mirrors')
        self.trigger('move_to_STATE_REBALANCE_MOVE_MIRRORS_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_MOVE_MIRRORS_DONE(self) -> None:
        if self.primary_segments_to_move:
            self.trigger('move_to_STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_STARTED')
        else:
            self.trigger('move_to_STATE_REBALANCE_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_STARTED(self) -> None:
        self.execute_role_swaps(self.primary_segments_to_move, self.RoleSwapDirection.PRIMARY_TO_MIRROR)
        self.trigger('move_to_STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_SWAP_PREFERRED_ROLES_PRIMARY_TO_MIRROR_DONE(self) -> None:
        self.trigger('move_to_STATE_REBALANCE_MOVE_PRIMARIES_STARTED')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_MOVE_PRIMARIES_STARTED(self) -> None:
        self.logger.info('Rebalance - start moving primaries')
        self.process_moves(self.moves_primaries)
        self.logger.info('Rebalance - end moving primaries')
        self.trigger('move_to_STATE_REBALANCE_MOVE_PRIMARIES_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_MOVE_PRIMARIES_DONE(self) -> None:
        self.trigger('move_to_STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_STARTED')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_STARTED(self) -> None:
        self.execute_role_swaps(self.primary_segments_to_move, self.RoleSwapDirection.MIRROR_TO_PRIMARY)
        self.trigger('move_to_STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_SWAP_PREFERRED_ROLES_MIRROR_TO_PRIMARY_DONE(self) -> None:
        self.trigger('move_to_STATE_REBALANCE_DONE')

    @wrap_state_func_with_faults
    def on_enter_STATE_REBALANCE_DONE(self) -> None:
        pass

    # state callbacks end here
