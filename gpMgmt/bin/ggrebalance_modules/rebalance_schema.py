#!/usr/bin/env python3

from psycopg2.extensions import cursor
from gppylib.db import dbconn
from gppylib.system.environment import GpCoordinatorEnvironment
from gppylib.utils import escape_string
from typing import List
from ggrebalance_modules.planner import Plan, deserializePlan
from ggrebalance_modules.rebalance_step import *

DBNAME = 'postgres'

STATE_NOT_DEFINED = 'not defined'

def get_table_distr_segment_count(conn: dbconn.Connection, schema_name: str, table_name: str) -> int:
    row = dbconn.queryRow(conn,
                          f'''SELECT p.numsegments
                          FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
                          JOIN gp_distribution_policy p ON c.oid = p.localoid
                          WHERE n.nspname='{escape_string(schema_name)}' AND c.relname='{escape_string(table_name)}';''')
    return int(row[0])

class RebalanceSchema:
    STATE_CATEGORY_SHRINK = 'SHRINK'
    STATE_CATEGORY_REBALANCE = 'REBALANCE'
    STATE_CATEGORY_MAIN = 'MAIN'

    schema_name = 'ggrebalance'
    rebalance_status = 'rebalance_status'
    table_rebalance_status_detail = 'table_rebalance_status_detail'
    rebalance_progress_view = 'rebalance_progress'
    saved_plan = 'saved_plan'
    segment_move_steps = 'segment_move_steps'

    class ProgressType(Enum):
        PROGRESS_NOT_DEFINED = 0
        PROGRESS_NO = 1
        PROGRESS_SIMPLE = 2
        PROGRESS_DETAILED = 3

    def __init__(self, conn: dbconn.Connection):
        self.conn = conn
        self.progress_type = self.ProgressType.PROGRESS_NOT_DEFINED

    def createSchema(self, plan: Plan, progress_type: ProgressType, shrink_rollback_states: List[str]) -> None:
        dbconn.execSQL(self.conn, 'BEGIN')
        dbconn.execSQL(self.conn, f'CREATE SCHEMA {self.schema_name}')
        dbconn.execSQL(self.conn,
                       f'''CREATE TABLE {self.schema_name}.{self.rebalance_status}
                       (state TEXT, state_category TEXT, updated TIMESTAMP WITH TIME ZONE)
                       DISTRIBUTED REPLICATED''')
        dbconn.execSQL(self.conn,
                       f'''CREATE TABLE {self.schema_name}.{self.table_rebalance_status_detail}
                       (db_name TEXT, schema_name TEXT, rel_name TEXT,
                       status TEXT,
                       rebalance_type TEXT,
                       rebalance_started TIMESTAMP WITH TIME ZONE,
                       rebalance_finished TIMESTAMP WITH TIME ZONE,
                       source_bytes NUMERIC,
                       CONSTRAINT unique_fqn UNIQUE (db_name, schema_name, rel_name))
                       DISTRIBUTED REPLICATED''')
        dbconn.execSQL(self.conn,
                       f'''CREATE TABLE {self.schema_name}.{self.saved_plan}
                       (plan BYTEA)
                       DISTRIBUTED REPLICATED''')

        dbconn.execSQL(self.conn,
                       f'''CREATE TABLE {self.schema_name}.{self.segment_move_steps}
                       (move_order INT NOT NULL UNIQUE, status TEXT, is_rollback BOOL, step BYTEA)
                       DISTRIBUTED REPLICATED''')

        self.savePlan(plan)

        self.progress_type = progress_type

        if self.progress_type != self.ProgressType.PROGRESS_NO:
            states_str = ',\n'.join(f"'{s}'" for s in shrink_rollback_states)
            dbconn.execSQL(self.conn, f'''
CREATE FUNCTION {self.schema_name}._is_rollback()
RETURNS boolean AS $$
    SELECT COALESCE(
        (SELECT state IN (
{states_str})
        FROM {self.schema_name}.{self.rebalance_status} WHERE state_category = 'SHRINK' ORDER BY updated DESC LIMIT 1),
        FALSE)
$$ LANGUAGE sql''')

        if self.progress_type == self.ProgressType.PROGRESS_SIMPLE:

            dbconn.execSQL(self.conn, f'''
CREATE VIEW {self.schema_name}.{self.rebalance_progress_view} AS
WITH
cond AS (SELECT {self.schema_name}._is_rollback() AS rollback_in_progress),
rebalance_progress_normal_flow AS
    (
    SELECT
        '1.1. Tables shrunk' AS stat_name,
        count(1)::text AS stat_value
    FROM {self.schema_name}.{self.table_rebalance_status_detail} WHERE status = 'done' AND rebalance_started IS NOT NULL AND rebalance_finished IS NOT NULL
    UNION
    SELECT
        '1.3. Tables left to shrink' AS stat_name,
        count(1)::text AS stat_value
    FROM {self.schema_name}.{self.table_rebalance_status_detail} WHERE status = 'none' AND rebalance_started IS NULL AND rebalance_finished IS NULL
    UNION
    SELECT
        '1.2. Tables shrink in progress' AS stat_name,
        count(1)::text AS stat_value
    FROM {self.schema_name}.{self.table_rebalance_status_detail} WHERE status = 'none' AND rebalance_started IS NOT NULL
    ORDER BY stat_name ASC
    ),
rebalance_progress_rollback_flow AS
    (
    SELECT
        '1.1. Tables rolled back' AS stat_name,
        count(1)::text AS stat_value
    FROM {self.schema_name}.{self.table_rebalance_status_detail} WHERE status = 'none' AND rebalance_started IS NOT NULL AND rebalance_finished IS NOT NULL
    UNION
    SELECT
        '1.2. Tables rollback in progress' AS stat_name,
        count(1)::text AS stat_value
    FROM {self.schema_name}.{self.table_rebalance_status_detail} WHERE status = 'done' AND rebalance_started IS NOT NULL AND rebalance_finished IS NULL
    UNION
    SELECT
        '1.3. Tables left to rollback' AS stat_name,
        count(1)::text AS stat_value
    FROM {self.schema_name}.{self.table_rebalance_status_detail} WHERE status = 'done' AND rebalance_started IS NULL
    ORDER BY stat_name ASC
    )
SELECT * FROM rebalance_progress_normal_flow WHERE NOT (SELECT rollback_in_progress FROM cond)
UNION ALL
SELECT * FROM rebalance_progress_rollback_flow WHERE (SELECT rollback_in_progress FROM cond);''')

        elif self.progress_type == self.ProgressType.PROGRESS_DETAILED:

            dbconn.execSQL(self.conn, f'''
CREATE VIEW {self.schema_name}.{self.rebalance_progress_view} AS
WITH
cond AS (SELECT {self.schema_name}._is_rollback() AS rollback_in_progress),
rebalance_progress_normal_flow AS
    (
    WITH
    stat_processed AS (
        SELECT
            count(1) as tables_processed,
            coalesce(sum(source_bytes), 0)::float AS bytes_processed,
            coalesce(sum(source_bytes) / EXTRACT(EPOCH FROM (max(rebalance_finished) - min(rebalance_started))), 0)::float AS est_processing_rate
        FROM {self.schema_name}.{self.table_rebalance_status_detail} WHERE status = 'done' AND rebalance_started IS NOT NULL AND rebalance_finished IS NOT NULL
        ),
    stat_in_processing AS (
        SELECT
            count(1) as tables_in_processing,
            coalesce(sum(source_bytes), 0) bytes_in_processing
        FROM {self.schema_name}.{self.table_rebalance_status_detail} WHERE status = 'none' AND rebalance_started IS NOT NULL
        ),
    stat_left_to_process AS (
        SELECT
            coalesce(sum(source_bytes), 0)::float as bytes_left_to_process
        FROM {self.schema_name}.{self.table_rebalance_status_detail} WHERE status = 'none'
        )
    SELECT
        '1.1. Tables shrunk' AS stat_name,
        tables_processed::text AS stat_value
    FROM stat_processed
    UNION
    SELECT
        '1.3. Tables left to shrink' AS stat_name,
        count(1)::text AS stat_value
    FROM {self.schema_name}.{self.table_rebalance_status_detail} WHERE status = 'none' AND rebalance_started IS NULL AND rebalance_finished IS NULL
    UNION
    SELECT
        '1.2. Tables shrink in progress' AS stat_name,
        tables_in_processing::text AS stat_value
    FROM stat_in_processing
    UNION
    SELECT
        '2.1. Bytes processed' AS stat_name,
        bytes_processed::text AS stat_value
    FROM stat_processed
    UNION
    SELECT
        '2.2. Bytes left to process' AS stat_name,
        bytes_left_to_process::text AS stat_value
    FROM stat_left_to_process
    UNION
    SELECT
        '2.3. Bytes in progress' AS stat_name,
        bytes_in_processing::text AS stat_value
    FROM stat_in_processing
    UNION
    SELECT
        '3.1. Estimated shrink rate' AS stat_name,
        (est_processing_rate / (1024*1024))::text || ' MB/s' AS stat_value
    FROM stat_processed
    UNION
    SELECT
        '3.2. Estimated time' AS stat_name,
        CASE
            WHEN p.est_processing_rate = 0 THEN 'not defined'
            ELSE (l.bytes_left_to_process / p.est_processing_rate)::text || ' s'
        END AS stat_value
    FROM stat_left_to_process l, stat_processed p 
    ORDER BY stat_name ASC
    ),
rebalance_progress_rollback_flow AS
    (
    WITH
    stat_processed AS (
        SELECT
            count(1) as tables_processed,
            coalesce(sum(source_bytes), 0)::float AS bytes_processed,
            coalesce(sum(source_bytes) / EXTRACT(EPOCH FROM (max(rebalance_finished) - min(rebalance_started))), 0)::float AS est_processing_rate
        FROM {self.schema_name}.{self.table_rebalance_status_detail} WHERE status = 'none' AND rebalance_started IS NOT NULL AND rebalance_finished IS NOT NULL
        ),
    stat_in_processing AS (
        SELECT
            count(1) as tables_in_processing,
            coalesce(sum(source_bytes), 0) bytes_in_processing
        FROM {self.schema_name}.{self.table_rebalance_status_detail} WHERE status = 'done' AND rebalance_started IS NOT NULL
        ),
    stat_left_to_process AS (
        SELECT
            coalesce(sum(source_bytes), 0)::float as bytes_left_to_process
        FROM {self.schema_name}.{self.table_rebalance_status_detail} WHERE status = 'done'
        )
    SELECT
        '1.1. Tables rolled back' AS stat_name,
        tables_processed::text AS stat_value
    FROM stat_processed
    UNION
    SELECT
        '1.2. Tables rollback in progress' AS stat_name,
        count(1)::text AS stat_value
    FROM {self.schema_name}.{self.table_rebalance_status_detail} WHERE status = 'done' AND rebalance_started IS NOT NULL AND rebalance_finished IS NULL
    UNION
    SELECT
        '1.3. Tables left to rollback' AS stat_name,
        count(1)::text AS stat_value
    FROM {self.schema_name}.{self.table_rebalance_status_detail} WHERE status = 'done' AND rebalance_started IS NULL
    UNION
    SELECT
        '2.1. Bytes processed' AS stat_name,
        bytes_processed::text AS stat_value
    FROM stat_processed
    UNION
    SELECT
        '2.2. Bytes left to process' AS stat_name,
        bytes_left_to_process::text AS stat_value
    FROM stat_left_to_process
    UNION
    SELECT
        '2.3. Bytes in progress' AS stat_name,
        bytes_in_processing::text AS stat_value
    FROM stat_in_processing
    UNION
    SELECT
        '3.1. Estimated shrink rate' AS stat_name,
        (est_processing_rate / (1024*1024))::text || ' MB/s' AS stat_value
    FROM stat_processed
    UNION
    SELECT
        '3.2. Estimated time' AS stat_name,
        CASE
            WHEN p.est_processing_rate = 0 THEN 'not defined'
            ELSE (l.bytes_left_to_process / p.est_processing_rate)::text || ' s'
        END AS stat_value
    FROM stat_left_to_process l, stat_processed p
    ORDER BY stat_name ASC
    )
SELECT * FROM rebalance_progress_normal_flow WHERE NOT (SELECT rollback_in_progress FROM cond)
UNION ALL
SELECT * FROM rebalance_progress_rollback_flow WHERE (SELECT rollback_in_progress FROM cond)''')

        dbconn.execSQL(self.conn, 'COMMIT')

    def dropSchema(self) -> None:
        dbconn.execSQL(self.conn, f'DROP SCHEMA {self.schema_name} CASCADE')

    def getSchemaName(self) -> str:
        return self.schema_name

    def savePlan(self, plan: Plan) -> None:
        dbconn.execSQL(self.conn,
                       f'''INSERT INTO {self.schema_name}.{self.saved_plan}
                       VALUES ('\\x{plan.serializePlan().hex()}')''')

    def retrieveSavedPlan(self) -> Plan:
        if not self.schemaExists():
            return None

        row = dbconn.queryRow(self.conn, f'SELECT count(1) FROM {self.schema_name}.{self.saved_plan}')
        plan_count = int(row[0])
        if plan_count > 1:
            raise Exception(f'Number of saved plans ({plan_count}) is > 1')
        if plan_count == 0:
            return None

        row = dbconn.queryRow(self.conn, f'SELECT plan FROM {self.schema_name}.{self.saved_plan} LIMIT 1')
        return deserializePlan(row[0])

    def schemaExists(self) -> bool:
        row = dbconn.queryRow(self.conn, f"SELECT COUNT(1) FROM pg_namespace WHERE nspname = '{self.schema_name}'")
        return int(row[0]) == 1

    def getStateFromPreviousRun(self, state_category: str) -> str:
        if self.schemaExists():
            cursor = dbconn.query(self.conn, f"SELECT state FROM {self.schema_name}.{self.rebalance_status} WHERE state_category = '{state_category}' ORDER BY updated DESC LIMIT 1")
            if cursor.rowcount > 0:
                return str(cursor.fetchone()[0])
        return STATE_NOT_DEFINED

    def getShrinkStateFromPreviousRun(self) -> str:
        return self.getStateFromPreviousRun(self.STATE_CATEGORY_SHRINK)

    def getRebalanceStateFromPreviousRun(self) -> str:
        return self.getStateFromPreviousRun(self.STATE_CATEGORY_REBALANCE)

    def getMainStateFromPreviousRun(self) -> str:
        return self.getStateFromPreviousRun(self.STATE_CATEGORY_MAIN)

    def isRollbackRebalanceFlow(self, rollback_start_state: str) -> bool:
        if self.schemaExists():
            row = dbconn.queryRow(self.conn,
                                  f"SELECT COUNT(1) FROM {self.schema_name}.{self.rebalance_status} "
                                  f"WHERE state_category = '{self.STATE_CATEGORY_REBALANCE}' "
                                  f"AND state = '{rollback_start_state}'")
            return int(row[0]) != 0
        return False

    def rebalanceSchema(self, target_segment_count: int) -> None:
        # Before rebalancing check if the tables are already rebalanced
        # (in case we re-enter after interruption that happened after COMMIT but before new state)
        if get_table_distr_segment_count(self.conn, self.schema_name, self.rebalance_status) > target_segment_count:
            dbconn.execSQL(self.conn,
                           f'''ALTER TABLE "{self.schema_name}"."{self.rebalance_status}"
                           REBALANCE {target_segment_count}''')

        if get_table_distr_segment_count(self.conn, self.schema_name, self.table_rebalance_status_detail) > target_segment_count:
            dbconn.execSQL(self.conn,
                           f'''ALTER TABLE "{self.schema_name}"."{self.table_rebalance_status_detail}"
                           REBALANCE {target_segment_count}''')

        if get_table_distr_segment_count(self.conn, self.schema_name, self.saved_plan) > target_segment_count:
            dbconn.execSQL(self.conn,
                           f'''ALTER TABLE "{self.schema_name}"."{self.saved_plan}"
                           REBALANCE {target_segment_count}''')

        if get_table_distr_segment_count(self.conn, self.schema_name, self.segment_move_steps) > target_segment_count:
            dbconn.execSQL(self.conn,
                           f'''ALTER TABLE "{self.schema_name}"."{self.segment_move_steps}"
                           REBALANCE {target_segment_count}''')

    def storeState(self, state: str, state_category: str) -> None:
        if self.schemaExists():
            dbconn.execSQL(self.conn,
                           f'''INSERT INTO {self.schema_name}.{self.rebalance_status}
                           VALUES ('{state}', '{state_category}', NOW())''')

    def storeShrinkState(self, state: str) -> None:
        self.storeState(state, self.STATE_CATEGORY_SHRINK)

    def storeRebalanceState(self, state: str) -> None:
        self.storeState(state, self.STATE_CATEGORY_REBALANCE)

    def storeMainState(self, state: str) -> None:
        self.storeState(state, self.STATE_CATEGORY_MAIN)

    def clearTablesToRebalanceWithStatus(self, status: str) -> None:
        dbconn.execSQL(self.conn,
                       f'''DELETE FROM {self.schema_name}.{self.table_rebalance_status_detail}
                       WHERE (status = '{status}')''')

    def addTableToRebalance(self, db: str, schema_name: str, rel_name: str, status: str, rel_size: int) -> None:
        dbconn.execSQL(self.conn,
                       f'''INSERT INTO {self.schema_name}.{self.table_rebalance_status_detail}
                       VALUES ('{escape_string(db)}', '{escape_string(schema_name)}', '{escape_string(rel_name)}', '{status}',
                                'SHRINK', NULL, NULL, {rel_size} )''')

    def setTableRebalanceStartTime(self, db: str, schema_name: str, rel_name: str) -> None:
        dbconn.execSQL(self.conn,
                       f'''UPDATE {self.schema_name}.{self.table_rebalance_status_detail} SET rebalance_started = now(), rebalance_finished = NULL
                       WHERE db_name = '{escape_string(db)}' AND schema_name = '{escape_string(schema_name)}' AND rel_name = '{escape_string(rel_name)}';''')

    def setStatusForTableToRebalance(self, db: str, schema_name: str, rel_name: str, status: str) -> None:
        dbconn.execSQL(self.conn,
                       f'''UPDATE {self.schema_name}.{self.table_rebalance_status_detail} SET status = '{status}', rebalance_finished = now()
                       WHERE db_name = '{escape_string(db)}' AND schema_name = '{escape_string(schema_name)}' AND rel_name = '{escape_string(rel_name)}';''')

    def getTablesToRebalanceWithStatus(self, status: str) -> cursor:
        return dbconn.query(self.conn, f"""SELECT db_name, schema_name, rel_name FROM
                            {self.schema_name}.{self.table_rebalance_status_detail} WHERE status = '{status}' ORDER BY db_name, schema_name, rel_name""")

    def saveExecutionSteps(self, steps: List[RebalanceStep]) -> None:
        dbconn.execSQL(self.conn, 'BEGIN')

        dbconn.execSQL(self.conn, f'TRUNCATE TABLE {self.schema_name}.{self.segment_move_steps}')
        
        for step in steps:
            dbconn.execSQL(self.conn,
                       f'''INSERT INTO {self.schema_name}.{self.segment_move_steps}
                       VALUES ({step.getMoveOrder()}, '{step.getStatus().name}', '{step.isRollback()}', '\\x{step.serializeStep().hex()}')''')

        dbconn.execSQL(self.conn, 'COMMIT')

    def updateExecutionStep(self, step: RebalanceStep) -> None:
        dbconn.execSQL(self.conn,
                       f'''UPDATE {self.schema_name}.{self.segment_move_steps}
                       SET status='{step.getStatus().name}', step='\\x{step.serializeStep().hex()}' WHERE move_order = {step.getMoveOrder()}''')

    def allExecutionStepsAreDone(self) -> bool:
        row = dbconn.queryRow(self.conn,
                              f"SELECT count(1) FROM {self.schema_name}.{self.segment_move_steps} "
                              f"WHERE status NOT IN ('{RebalanceStep.Status.DONE.name}', '{RebalanceStep.Status.CANCELLED.name}')")
        not_done_count = int(row[0])
        return not_done_count == 0

    def getExecutionSteps(self, status_filter: List[RebalanceStep.Status]) -> List[RebalanceStep]:
        result = []

        filter = ""
        if len(status_filter) > 0:
            status_list = ', '.join("'" +  status.name + "'" for status in status_filter)
            filter = f" WHERE status IN ({status_list})"

        cursor = dbconn.query(self.conn,
                              f'SELECT step FROM {self.schema_name}.{self.segment_move_steps} {filter} ORDER BY move_order')
        for row in cursor:
            result.append(deserializeStep(row[0]))

        return result

    def getProgressType(self) -> ProgressType:
        if self.progress_type != self.ProgressType.PROGRESS_NOT_DEFINED:
            return self.progress_type
        # We check the progress type basing on the existence of the progress view
        # and number of values there.
        if 0 == int(dbconn.querySingleton(self.conn,
            f"SELECT CASE WHEN to_regclass('{self.schema_name}.{self.rebalance_progress_view}') IS NULL THEN 0 ELSE 1 END AS view_exists")):
            self.progress_type = self.ProgressType.PROGRESS_NO
        else:
            if 3 >= int(dbconn.querySingleton(self.conn,
                f"SELECT count(1) FROM {self.schema_name}.{self.rebalance_progress_view}")):
                self.progress_type = self.ProgressType.PROGRESS_SIMPLE
            else:
                self.progress_type = self.ProgressType.PROGRESS_DETAILED
        return self.progress_type

    @classmethod
    def checkOperationInProgress(cls, gpEnv: GpCoordinatorEnvironment) -> bool:
        """Checks if there is ggrebalance operation in progress (possibly interrupted)"""
        dburl = dbconn.DbURL(dbname=DBNAME, port=gpEnv.getCoordinatorPort())
        with closing(dbconn.connect(dburl, encoding='UTF8')) as conn:
            # if there is no schema existing, we are not performing shrink or rebalance
            if 0 == (dbconn.querySingleton(conn, f"SELECT COUNT(1) FROM pg_namespace WHERE nspname = '{cls.schema_name}'")):
                return False
            # if there is schema existing, check the last state - if it is final one - then we've complete all operations
            latest_main_state = ''
            cursor = dbconn.query(conn, f"SELECT state FROM {cls.schema_name}.{cls.rebalance_status} WHERE state_category = '{cls.STATE_CATEGORY_MAIN}' ORDER BY updated DESC LIMIT 1")
            if cursor.rowcount > 0:
                latest_main_state = str(cursor.fetchone()[0])
            if latest_main_state == 'STATE_EXECUTOR_DONE':
                return False

        return True
