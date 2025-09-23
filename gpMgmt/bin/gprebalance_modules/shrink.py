#!/usr/bin/env python3

from transitions import Machine
from contextlib import closing
from typing import Any

try:
    from gppylib.commands.unix import *
    from gppylib.commands.gp import *
    from gppylib.gplog import *
    from gppylib.db import dbconn
    from gppylib.userinput import *
    from gppylib.commands import base
    from gppylib.commands.gp import SEGMENT_STOP_TIMEOUT_DEFAULT, SegmentStop
    from gppylib.system.environment import *
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

    rebalance_schema_name = 'ggrebalance'
    rebalance_status = 'rebalance_status'
    table_rebalance_status_detail = 'table_rebalance_status_detail'

    states = [
        'STATE_START',
        'STATE_OPTIONS_VALIDATION',
        'STATE_CHECK_PREVIOUS_RUN',
        'STATE_END',
        'STATE_CLEANUP',
        'STATE_ERROR',
        'STATE_ROLLBACK'
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
            'trigger': 'move_to_STATE_CHECK_PREVIOUS_RUN',
            'source': 'STATE_OPTIONS_VALIDATION',
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
            'source': ['STATE_SHRINK_DONE', 'STATE_CHECK_PREVIOUS_RUN', 'STATE_CLEANUP'],
            'dest': 'STATE_END'
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

        self.machine = Machine(model = self,
                               queued=True,
                               states = self.states + self.states_main_shrink_flow,
                               transitions = self.transitions,
                               initial = 'STATE_START',
                               before_state_change = 'on_every_state')

    def run(self) -> None:
        self.trigger('start')

    def ggrebalance_schema_exists(self) -> bool:
        row = dbconn.queryRow(self.conn, f"SELECT COUNT(1) FROM pg_namespace WHERE nspname = '{self.rebalance_schema_name}'")
        return int(row[0]) == 1

    def get_state_from_previous_run(self) -> str:
        cursor = dbconn.query(self.conn, f'SELECT status FROM {self.rebalance_schema_name}.{self.rebalance_status} ORDER BY updated DESC LIMIT 1')
        if cursor.rowcount > 0:
            return str(cursor.fetchone()[0])
        return 'not defined'

    def on_every_state(self) -> None:
        if self.shutdown_requested:
            self.logger.info('Shrink was interrupted')
            sys.exit(1)

        assert self.state in self.states + self.states_main_shrink_flow

        # insert status if the schema already exists
        if self.state in self.states_main_shrink_flow:
            if self.ggrebalance_schema_exists():
                dbconn.execSQL(self.conn,
                               f'''INSERT INTO {self.rebalance_schema_name}.{self.rebalance_status}
                               VALUES ('{self.state}', NOW())''')

    # state callbacks start here
    def on_enter_STATE_OPTIONS_VALIDATION(self) -> None:
        if self.options.clean_required:
            self.trigger('move_to_STATE_CLEANUP')
        else:
            self.trigger('move_to_STATE_CHECK_PREVIOUS_RUN')

    def on_enter_STATE_CHECK_PREVIOUS_RUN(self) -> None:
        # check if rebalance schema exists
        # and whether we can get the state where we stopped in previous run
        # in order to proceed from the same point
        if self.ggrebalance_schema_exists():
            self.logger.info('Rebalance schema already exists')
            state_from_prev_run = self.get_state_from_previous_run()
            # check maybe the state is the final one
            if state_from_prev_run == self.states_main_shrink_flow[-1]:
                self.logger.info('Previous run was completed successfully. Please execute cleanup before a new run.')
                self.trigger('move_to_STATE_END')
            else:
                self.logger.info(f"Previous run stopped after state '{state_from_prev_run}', trying to continue from the next state...")
                try:
                    next_state = self.states_main_shrink_flow[ self.states_main_shrink_flow.index(state_from_prev_run) + 1 ]
                except:
                    self.logger.error("Can't determine next state")
                    self.trigger('move_to_STATE_ERROR')
                    return
                # use auto to_«state» method to recover
                self.trigger(f'to_{next_state}')
        else:
            self.trigger('move_to_STATE_SETUP_SHRINK_SCHEMA_STARTED')

    def on_enter_STATE_SETUP_SHRINK_SCHEMA_STARTED(self) -> None:
        # Create schema and status tables
        dbconn.execSQL(self.conn, 'BEGIN')
        dbconn.execSQL(self.conn, f'DROP SCHEMA IF EXISTS {self.rebalance_schema_name} CASCADE')
        dbconn.execSQL(self.conn, f'CREATE SCHEMA {self.rebalance_schema_name}')
        dbconn.execSQL(self.conn,
                       f'''CREATE TABLE {self.rebalance_schema_name}.{self.rebalance_status}
                       (status TEXT, updated TIMESTAMP WITH TIME ZONE)
                       DISTRIBUTED REPLICATED''')
        dbconn.execSQL(self.conn,
                       f'''CREATE TABLE {self.rebalance_schema_name}.{self.table_rebalance_status_detail}
                       (db_name TEXT, schema_name TEXT, rel_name TEXT, status TEXT,
                       CONSTRAINT unique_fqn UNIQUE (db_name, schema_name, rel_name))
                       DISTRIBUTED REPLICATED''')
        dbconn.execSQL(self.conn, 'COMMIT')

        self.trigger('move_to_STATE_SETUP_SHRINK_SCHEMA_DONE')

    def on_enter_STATE_SETUP_SHRINK_SCHEMA_DONE(self) -> None:
        self.logger.info(f'Created shrink schema {self.rebalance_schema_name}')
        self.trigger('move_to_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_STARTED')

    def get_table_distr_segment_count(self, schema_name, table_name) -> int:
        row = dbconn.queryRow(self.conn,
                              f'''SELECT p.numsegments
                              FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
                              JOIN gp_distribution_policy p ON c.oid = p.localoid
                              WHERE n.nspname='{schema_name}' AND c.relname='{table_name}';''')
        return int(row[0])

    def on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_STARTED(self) -> None:
        dbconn.execSQL(self.conn, 'BEGIN')
        dbconn.execSQL(self.conn, 'SELECT gp_expand_lock_catalog()')
        dbconn.execSQL(self.conn, 'CHECKPOINT')
        dbconn.execSQL(self.conn, f'SELECT gp_toolkit.gp_set_rebalance_numsegments({self.options.target_segment_count})')

        self.gparray.dumpToFile(self.gparray_dump_file)

        # Rebalance the status tables we've created previously right here before we start to rebalance all other tables.
        # Before that check if the tables are already rebalanced
        # (in case we re-enter after interruption that happened after COMMIT but before new state)
        if self.get_table_distr_segment_count(self.rebalance_schema_name,
                                              self.rebalance_status) > self.options.target_segment_count:
            dbconn.execSQL(self.conn,
                           f'''ALTER TABLE "{self.rebalance_schema_name}"."{self.rebalance_status}"
                           REBALANCE {self.options.target_segment_count}''')

        if self.get_table_distr_segment_count(self.rebalance_schema_name,
                                              self.table_rebalance_status_detail) > self.options.target_segment_count:
            dbconn.execSQL(self.conn,
                           f'''ALTER TABLE "{self.rebalance_schema_name}"."{self.table_rebalance_status_detail}"
                           REBALANCE {self.options.target_segment_count}''')

        dbconn.execSQL(self.conn, 'COMMIT')

        self.trigger('move_to_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_DONE')

    def on_enter_STATE_BACKUP_CATALOG_AND_UPDATE_TARGET_SEGMENT_COUNT_DONE(self) -> None:
        self.logger.info(f'Updated target segment count to {self.options.target_segment_count}')
        self.trigger('move_to_STATE_PREPARE_SHRINK_SCHEMA_STARTED')

    def on_enter_STATE_PREPARE_SHRINK_SCHEMA_STARTED(self) -> None:
        # collect databases and tables that require 'ALTER TABLE REBALANCE'
        # and store in 'table_rebalance_status_detail' table

        dbconn.execSQL(self.conn, 'BEGIN')

        # cleanup table_rebalance_status_detail for the case we re-enter this state after we were interrupted right after it
        dbconn.execSQL(self.conn, f'TRUNCATE {self.rebalance_schema_name}.{self.table_rebalance_status_detail}')

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
                                      p.numsegments > {self.options.target_segment_count} AND
                                      n.nspname NOT IN ('pg_catalog', 'information_schema', '{self.rebalance_schema_name}')''')
                for schema_name, rel_name in cursor:
                    dbconn.execSQL(self.conn,
                                   f'''INSERT INTO {self.rebalance_schema_name}.{self.table_rebalance_status_detail}
                                   VALUES ('{db}', '{schema_name}', '{rel_name}', 'none')''')

        dbconn.execSQL(self.conn, 'COMMIT')

        self.trigger('move_to_STATE_PREPARE_SHRINK_SCHEMA_DONE')

    def on_enter_STATE_PREPARE_SHRINK_SCHEMA_DONE(self) -> None:
        self.logger.info(f'Initiated {self.rebalance_schema_name}.{self.table_rebalance_status_detail}')
        self.trigger('move_to_STATE_SHRINK_TABLES_STARTED')

    class TableRebalanceTask(SQLCommand):
        def __init__(self,
                     shrink: 'GGShrink',
                     db_name: str,
                     schema_name: str,
                     rel_name: str) -> None:
            self.shrink = shrink
            self.db_name = db_name
            self.schema_name = schema_name
            self.rel_name = rel_name
            SQLCommand.__init__(self, f'task rebalance for {self.db_name}.{self.schema_name}.{self.rel_name}')

        def run(self) -> None:
            dburl = dbconn.DbURL(dbname=self.db_name, port=self.shrink.gpEnv.getCoordinatorPort())
            with closing(dbconn.connect(dburl, encoding='UTF8')) as conn:
                dbconn.execSQL(conn, 'BEGIN')
                dbconn.execSQL(conn,
                               f'''ALTER TABLE "{self.schema_name}"."{self.rel_name}"
                               REBALANCE {self.shrink.options.target_segment_count}''')
                dbconn.execSQL(self.shrink.conn,
                               f'''UPDATE {self.shrink.rebalance_schema_name}.{self.shrink.table_rebalance_status_detail} SET status = 'done'
                               WHERE db_name = '{self.db_name}' AND schema_name = '{self.schema_name}' AND rel_name = '{self.rel_name}';''')
                dbconn.execSQL(conn, 'COMMIT')
            self.set_results(CommandResult(0, b'', b'', True, False))

    def on_enter_STATE_SHRINK_TABLES_STARTED(self) -> None:
        self.logger.info('Start tables rebalance')

        # perform 'ALTER TABLE REBALANCE' for all not yet processed tables
        cursor = dbconn.query(self.conn,
                              f"SELECT db_name, schema_name, rel_name FROM {self.rebalance_schema_name}.{self.table_rebalance_status_detail} WHERE status = 'none'")

        self.logger.info(f'Tables to process {cursor.rowcount}')

        if cursor.rowcount > 0:
            self.workers_for_tables_rebalance = WorkerPool(numWorkers=min(cursor.rowcount, self.options.parallel))

            for db_name, schema_name, rel_name in cursor:
                task = self.TableRebalanceTask(self,
                                               db_name,
                                               schema_name,
                                               rel_name)
                self.workers_for_tables_rebalance.addCommand(task)

            print_progress(self.workers_for_tables_rebalance, interval=1)

            self.workers_for_tables_rebalance.haltWork()
            self.workers_for_tables_rebalance.joinWorkers()

            for task in self.workers_for_tables_rebalance.getCompletedItems():
                if not task.was_successful():
                    raise Exception(f'Failed to do ALTER REBALANCE: {task.get_results().stderr}')

            self.workers_for_tables_rebalance = None

        self.trigger('move_to_STATE_SHRINK_TABLES_DONE')

    def on_enter_STATE_SHRINK_TABLES_DONE(self) -> None:
        self.logger.info('Tables rebalance complete')
        self.trigger('move_to_STATE_SHRINK_CATALOG_STARTED')

    def on_enter_STATE_SHRINK_CATALOG_STARTED(self) -> None:
        self.logger.info('Start catalog shrink')

        ## Shrink catalog
        dbconn.execSQL(self.conn, 'BEGIN')
        cursor = dbconn.execSQL(self.conn, 'SELECT gp_expand_lock_catalog()')
        dbconn.execSQL(self.conn, f'DELETE FROM gp_segment_configuration WHERE content >= {self.options.target_segment_count}')
        dbconn.execSQL(self.conn, 'CHECKPOINT')
        dbconn.execSQL(self.conn, 'SELECT gp_expand_bump_version()')
        cursor = dbconn.query(self.conn, 'SELECT gp_toolkit.gp_reset_rebalance_numsegments()')
        dbconn.execSQL(self.conn, 'COMMIT')

        self.trigger('move_to_STATE_SHRINK_CATALOG_DONE')

    def on_enter_STATE_SHRINK_CATALOG_DONE(self) -> None:
        self.logger.info('Catalog shrink complete')
        self.trigger('move_to_STATE_SHRINK_SEGMENTS_STOP_STARTED')

    def on_enter_STATE_SHRINK_SEGMENTS_STOP_STARTED(self) -> None:
        self.logger.info('Stopping shrinked segments...')

        segments_to_stop = self.gparray.get_segment_count() - self.options.target_segment_count
        segments_to_stop = segments_to_stop * 2 # consider mirrors
        self.workers_for_segment_stop = WorkerPool(numWorkers=min(segments_to_stop, self.options.batch_size))

        for seg_pair in self.gparray.getSegmentList():
            primary_seg = seg_pair.primaryDB
            mirror_seq = seg_pair.mirrorDB
            if primary_seg.getSegmentContentId() >= self.options.target_segment_count:
                if primary_seg.isSegmentUp():
                    cmd = SegmentStop(f'stop primary (content {primary_seg.getSegmentContentId()}, dbid {primary_seg.getSegmentDbId()})',
                                       primary_seg.getSegmentDataDirectory(),
                                       mode=self.stop_mode,
                                       timeout=self.timeout,
                                       ctxt=base.REMOTE,
                                       remoteHost=primary_seg.getSegmentHostName())
                    self.workers_for_segment_stop.addCommand(cmd)

                if mirror_seq != None and mirror_seq.isSegmentUp():
                    cmd = SegmentStop(f'stop mirror (content {mirror_seq.getSegmentContentId()}, dbid {mirror_seq.getSegmentDbId()})',
                                       mirror_seq.getSegmentDataDirectory(),
                                       mode=self.stop_mode,
                                       timeout=self.timeout,
                                       ctxt=base.REMOTE,
                                       remoteHost=mirror_seq.getSegmentHostName())
                    self.workers_for_segment_stop.addCommand(cmd)

        print_progress(self.workers_for_segment_stop, interval=1)

        self.workers_for_segment_stop.haltWork()
        self.workers_for_segment_stop.joinWorkers()

        for task in self.workers_for_segment_stop.getCompletedItems():
            if not task.was_successful():
                raise Exception('Failed to stop segments')

        self.workers_for_segment_stop = None

        self.trigger('move_to_STATE_SHRINK_SEGMENTS_STOP_DONE')

    def on_enter_STATE_SHRINK_SEGMENTS_STOP_DONE(self) -> None:
        self.logger.info('Shrinked segments were stopped')
        self.trigger('move_to_STATE_SHRINK_DONE')

    def on_enter_STATE_SHRINK_DONE(self) -> None:
        os.remove(self.gparray_dump_file)
        self.logger.info('Shrink is complete')
        self.trigger('move_to_STATE_END')

    def on_enter_STATE_CLEANUP(self) -> None:
        dbconn.execSQL(self.conn, f'DROP SCHEMA {self.rebalance_schema_name} CASCADE')
        self.logger.info('Cleanup is complete')
        self.trigger('move_to_STATE_END')

    def on_enter_STATE_END(self) -> None:
        self.conn.close()

    def on_enter_STATE_ERROR(self) -> None:
        sys.exit(1)

    # state callbacks end here

    def shutdown(self) -> None:
        if self.workers_for_tables_rebalance != None:
            self.workers_for_tables_rebalance.haltWork()
            self.workers_for_tables_rebalance.joinWorkers()

        if self.workers_for_segment_stop != None:
            self.workers_for_segment_stop.haltWork()
            self.workers_for_segment_stop.joinWorkers()

        self.shutdown_requested = True
