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

@dataclass
class RebalanceSummaryInfo:
    executed_rebalance_steps: List[RebalanceStep]

    tables_shrunk: str = '0'
    bytes_processed: str = '0'
    shrink_rate: str = '0 MB/s'
    shrink_total_time: str = '0s'

    is_rollback: bool = False
    tables_rolled_back: str = '0'
    rollback_rate: str = '0 MB/s'
    rollback_total_time: str = '0s'

class RebalanceSchema:
    STATE_CATEGORY_SHRINK = 'SHRINK'
    STATE_CATEGORY_REBALANCE = 'REBALANCE'
    STATE_CATEGORY_MAIN = 'MAIN'

    # possible stat names for rebalance_progress_view
    STAT_NAME_1_1_TABLES_SHRUNK = '1.1. Tables shrunk'
    STAT_NAME_1_2_TABLES_SHRINK_IN_PROGRESS = '1.2. Tables shrink in progress'
    STAT_NAME_1_3_TABLES_LEFT_TO_SHRINK = '1.3. Tables left to shrink'
    STAT_NAME_1_1_TABLES_ROLLED_BACK = '1.1. Tables rolled back'
    STAT_NAME_1_2_TABLES_ROLLBACK_IN_PROGRESS = '1.2. Tables rollback in progress'
    STAT_NAME_1_3_TABLES_LEFT_TO_ROLLBACK = '1.3. Tables left to rollback'
    STAT_NAME_2_1_BYTES_PROCESSED = '2.1. Bytes processed'
    STAT_NAME_2_2_BYTES_LEFT_TO_PROCESS = '2.2. Bytes left to process'
    STAT_NAME_2_3_BYTES_IN_PROGRESS = '2.3. Bytes in progress'
    STAT_NAME_3_1_ESTIMATED_SHRINK_RATE = '3.1. Estimated shrink rate'
    STAT_NAME_3_2_ESTIMATED_TIME = '3.2. Estimated time'
    STAT_NAME_SHRINK_TOTAL_TIME = 'Shrink total time'

    schema_name = 'ggrebalance'
    rebalance_status = 'rebalance_status'
    table_rebalance_status_detail = 'table_rebalance_status_detail'
    rebalance_progress_view = 'rebalance_progress'
    rebalance_progress_view_history = 'rebalance_progress_history'
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
        self.progress_summary = None

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
                       DISTRIBUTED BY (rel_name)''')
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
        '{self.STAT_NAME_1_1_TABLES_SHRUNK}' AS stat_name,
        count(1)::text AS stat_value
    FROM {self.schema_name}.{self.table_rebalance_status_detail} WHERE status = 'done' AND rebalance_started IS NOT NULL AND rebalance_finished IS NOT NULL
    UNION
    SELECT
        '{self.STAT_NAME_1_3_TABLES_LEFT_TO_SHRINK}' AS stat_name,
        count(1)::text AS stat_value
    FROM {self.schema_name}.{self.table_rebalance_status_detail} WHERE status = 'none' AND rebalance_started IS NULL AND rebalance_finished IS NULL
    UNION
    SELECT
        '{self.STAT_NAME_1_2_TABLES_SHRINK_IN_PROGRESS}' AS stat_name,
        count(1)::text AS stat_value
    FROM {self.schema_name}.{self.table_rebalance_status_detail} WHERE status = 'none' AND rebalance_started IS NOT NULL
    ORDER BY stat_name ASC
    ),
rebalance_progress_rollback_flow AS
    (
    SELECT
        '{self.STAT_NAME_1_1_TABLES_ROLLED_BACK}' AS stat_name,
        count(1)::text AS stat_value
    FROM {self.schema_name}.{self.table_rebalance_status_detail} WHERE status = 'none' AND rebalance_started IS NOT NULL AND rebalance_finished IS NOT NULL
    UNION
    SELECT
        '{self.STAT_NAME_1_2_TABLES_ROLLBACK_IN_PROGRESS}' AS stat_name,
        count(1)::text AS stat_value
    FROM {self.schema_name}.{self.table_rebalance_status_detail} WHERE status = 'done' AND rebalance_started IS NOT NULL AND rebalance_finished IS NULL
    UNION
    SELECT
        '{self.STAT_NAME_1_3_TABLES_LEFT_TO_ROLLBACK}' AS stat_name,
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
        '{self.STAT_NAME_1_1_TABLES_SHRUNK}' AS stat_name,
        tables_processed::text AS stat_value
    FROM stat_processed
    UNION
    SELECT
        '{self.STAT_NAME_1_3_TABLES_LEFT_TO_SHRINK}' AS stat_name,
        count(1)::text AS stat_value
    FROM {self.schema_name}.{self.table_rebalance_status_detail} WHERE status = 'none' AND rebalance_started IS NULL AND rebalance_finished IS NULL
    UNION
    SELECT
        '{self.STAT_NAME_1_2_TABLES_SHRINK_IN_PROGRESS}' AS stat_name,
        tables_in_processing::text AS stat_value
    FROM stat_in_processing
    UNION
    SELECT
        '{self.STAT_NAME_2_1_BYTES_PROCESSED}' AS stat_name,
        bytes_processed::text AS stat_value
    FROM stat_processed
    UNION
    SELECT
        '{self.STAT_NAME_2_2_BYTES_LEFT_TO_PROCESS}' AS stat_name,
        bytes_left_to_process::text AS stat_value
    FROM stat_left_to_process
    UNION
    SELECT
        '{self.STAT_NAME_2_3_BYTES_IN_PROGRESS}' AS stat_name,
        bytes_in_processing::text AS stat_value
    FROM stat_in_processing
    UNION
    SELECT
        '{self.STAT_NAME_3_1_ESTIMATED_SHRINK_RATE}' AS stat_name,
        (est_processing_rate / (1024*1024))::text || ' MB/s' AS stat_value
    FROM stat_processed
    UNION
    SELECT
        '{self.STAT_NAME_3_2_ESTIMATED_TIME}' AS stat_name,
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
        '{self.STAT_NAME_1_1_TABLES_ROLLED_BACK}' AS stat_name,
        tables_processed::text AS stat_value
    FROM stat_processed
    UNION
    SELECT
        '{self.STAT_NAME_1_2_TABLES_ROLLBACK_IN_PROGRESS}' AS stat_name,
        count(1)::text AS stat_value
    FROM {self.schema_name}.{self.table_rebalance_status_detail} WHERE status = 'done' AND rebalance_started IS NOT NULL AND rebalance_finished IS NULL
    UNION
    SELECT
        '{self.STAT_NAME_1_3_TABLES_LEFT_TO_ROLLBACK}' AS stat_name,
        count(1)::text AS stat_value
    FROM {self.schema_name}.{self.table_rebalance_status_detail} WHERE status = 'done' AND rebalance_started IS NULL
    UNION
    SELECT
        '{self.STAT_NAME_2_1_BYTES_PROCESSED}' AS stat_name,
        bytes_processed::text AS stat_value
    FROM stat_processed
    UNION
    SELECT
        '{self.STAT_NAME_2_2_BYTES_LEFT_TO_PROCESS}' AS stat_name,
        bytes_left_to_process::text AS stat_value
    FROM stat_left_to_process
    UNION
    SELECT
        '{self.STAT_NAME_2_3_BYTES_IN_PROGRESS}' AS stat_name,
        bytes_in_processing::text AS stat_value
    FROM stat_in_processing
    UNION
    SELECT
        '{self.STAT_NAME_3_1_ESTIMATED_SHRINK_RATE}' AS stat_name,
        (est_processing_rate / (1024*1024))::text || ' MB/s' AS stat_value
    FROM stat_processed
    UNION
    SELECT
        '{self.STAT_NAME_3_2_ESTIMATED_TIME}' AS stat_name,
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
        # We call getProgressSummary() here in order to memoize the summary
        # before we drop the schema, as we might need to show summary when the schema is already absent.
        self.getProgressSummary()
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

    def backupShrinkProgress(self) -> None:
        if self.getProgressType() != self.ProgressType.PROGRESS_NO:
            shrink_total_time = self.getShrinkTotalTime()
            dbconn.execSQL(self.conn,
                           f'DROP TABLE IF EXISTS {self.schema_name}.{self.rebalance_progress_view_history}')
            dbconn.execSQL(self.conn,
                           f'CREATE TABLE {self.schema_name}.{self.rebalance_progress_view_history} AS SELECT * FROM {self.schema_name}.{self.rebalance_progress_view}')
            # also add to progress history table the shrink total time to show it in the final summary
            dbconn.execSQL(self.conn,
                           f"INSERT INTO {self.schema_name}.{self.rebalance_progress_view_history} VALUES ('{self.STAT_NAME_SHRINK_TOTAL_TIME}', '{shrink_total_time}')")

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

    def getShrinkTotalTime(self, is_rollback: bool = False) -> str:
        target_status = 'done' if not is_rollback else 'none'
        if not is_rollback:
            filter_condition = "cte.state != 'STATE_SHRINK_TABLES_STARTED'"
        else:
            filter_condition = "cte.state != 'STATE_SHRINK_ROLLBACK_SHRINKED_TABLES_START' AND cte.state LIKE 'STATE_SHRINK_ROLLBACK_%'"

        # In order to get total shrink (or shrink rollback) time, we need to consider several items:
        # 1. the execution can be interrupted, so we can't just take the 'updated' column of last state and subtract the first state from it.
        # So we calculate the duration for each shrink state separately by subtracting the time of the preceeding state,
        # and then summarize them (refer to cte_other_states.duration). But! Here is another consideration:
        # 2. We can't do it for the state where the tables redistribution is done - as on each re-enter of this state
        # we process only the remaining amount of tables, and we will capture only the last re-enter duration.
        # Therefore we process table redistribution time separately - we get it from actual table rebalance_started and rebalance_finished
        # (taking into account their parallel execution). Refer to cte_tables_redistribution.duration.
        return dbconn.querySingleton(self.conn, f"""
WITH
cte_tables_redistribution AS (
    WITH events AS (
        SELECT rebalance_started AS ts, 1 AS delta FROM {self.schema_name}.{self.table_rebalance_status_detail}
        WHERE status = '{target_status}' and rebalance_finished IS NOT NULL
        UNION ALL
        SELECT rebalance_finished AS ts, -1 AS delta FROM {self.schema_name}.{self.table_rebalance_status_detail}
        WHERE status = '{target_status}' and rebalance_finished IS NOT NULL
    ),
    ordered AS (
        SELECT
            ts,
            sum(delta) OVER (ORDER BY ts) AS active,
            LEAD(ts) OVER (ORDER BY ts) AS next_ts
        FROM events
    )
    SELECT
        COALESCE(sum(next_ts - ts), INTERVAL '0') AS duration
    FROM ordered
    WHERE active > 0 AND next_ts IS NOT NULL
),
cte_other_states AS (
    WITH
    cte AS (
        SELECT state, state_category, updated, updated - LAG(updated) OVER (ORDER BY updated) AS duration
        FROM {self.schema_name}.{self.rebalance_status} ORDER BY updated
    )
    SELECT sum(cte.duration) as duration FROM cte WHERE cte.state_category = 'SHRINK' AND
    {filter_condition}
),
cte_total AS (
    SELECT date_trunc('second', cte_tables_redistribution.duration + cte_other_states.duration) AS duration from cte_tables_redistribution, cte_other_states
)
SELECT
EXTRACT(DAY FROM cte_total.duration)::text || 'd ' ||
EXTRACT(HOUR FROM cte_total.duration)::text || 'h' ||
EXTRACT(MINUTE FROM cte_total.duration)::text || 'm' ||
EXTRACT(SECOND FROM cte_total.duration)::text || 's'
AS total_duration FROM cte_total""")

    def getProgressSummary(self) -> RebalanceSummaryInfo:
        if self.progress_summary != None:
            return self.progress_summary

        result = RebalanceSummaryInfo(self.getExecutionSteps([]))

        if self.getProgressType() != self.ProgressType.PROGRESS_NO:
            result.is_rollback = bool(dbconn.querySingleton(self.conn,
                f"SELECT CASE WHEN to_regclass('{self.schema_name}.{self.rebalance_progress_view_history}') IS NULL THEN 0 ELSE 1 END AS view_backup_exists"))

            cursor = dbconn.query(self.conn,
                                f"SELECT stat_name, stat_value FROM {self.schema_name}.{self.rebalance_progress_view}")
            if not result.is_rollback:
                for stat_name, stat_value in cursor:
                    match stat_name:
                        case self.STAT_NAME_1_1_TABLES_SHRUNK:
                            result.tables_shrunk = stat_value
                        case self.STAT_NAME_2_1_BYTES_PROCESSED:
                            result.bytes_processed = stat_value
                        case self.STAT_NAME_3_1_ESTIMATED_SHRINK_RATE:
                            result.shrink_rate = stat_value

                result.shrink_total_time = self.getShrinkTotalTime()
            else:
                for stat_name, stat_value in cursor:
                    match stat_name:
                        case self.STAT_NAME_1_1_TABLES_ROLLED_BACK:
                            result.tables_rolled_back = stat_value
                        case self.STAT_NAME_2_1_BYTES_PROCESSED:
                            result.bytes_processed = stat_value
                        case self.STAT_NAME_3_1_ESTIMATED_SHRINK_RATE:
                            result.rollback_rate = stat_value

                cursor = dbconn.query(self.conn,
                                f"SELECT stat_name, stat_value FROM {self.schema_name}.{self.rebalance_progress_view_history}")
                for stat_name, stat_value in cursor:
                    match stat_name:
                        case self.STAT_NAME_1_1_TABLES_SHRUNK:
                            result.tables_shrunk = stat_value
                        case self.STAT_NAME_3_1_ESTIMATED_SHRINK_RATE:
                            result.shrink_rate = stat_value
                        case self.STAT_NAME_SHRINK_TOTAL_TIME:
                            result.shrink_total_time = stat_value

                result.rollback_total_time = self.getShrinkTotalTime(is_rollback = True)

        self.progress_summary = result
        return self.progress_summary

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
