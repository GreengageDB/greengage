#!/usr/bin/env python3

from psycopg2.extensions import cursor
from gppylib.db import dbconn
from gprebalance_modules.planner import Plan, deserializePlan

STATE_NOT_DEFINED = 'not defined'

def get_table_distr_segment_count(conn: dbconn.Connection, schema_name: str, table_name: str) -> int:
    row = dbconn.queryRow(conn,
                          f'''SELECT p.numsegments
                          FROM pg_class c JOIN pg_namespace n ON c.relnamespace = n.oid
                          JOIN gp_distribution_policy p ON c.oid = p.localoid
                          WHERE n.nspname='{schema_name}' AND c.relname='{table_name}';''')
    return int(row[0])

class RebalanceSchema:
    STATE_CATEGORY_SHRINK = 'SHRINK'
    STATE_CATEGORY_REBALANCE = 'REBALANCE'
    STATE_CATEGORY_MAIN = 'MAIN'

    def __init__(self, conn: dbconn.Connection):
        self.schema_name = 'ggrebalance'
        self.rebalance_status = 'rebalance_status'
        self.table_rebalance_status_detail = 'table_rebalance_status_detail'
        self.saved_plan = 'saved_plan'
        self.conn = conn

    def createSchema(self, plan: Plan) -> None:
        dbconn.execSQL(self.conn, 'BEGIN')
        dbconn.execSQL(self.conn, f'CREATE SCHEMA {self.schema_name}')
        dbconn.execSQL(self.conn,
                       f'''CREATE TABLE {self.schema_name}.{self.rebalance_status}
                       (state TEXT, state_category TEXT, updated TIMESTAMP WITH TIME ZONE)
                       DISTRIBUTED REPLICATED''')
        dbconn.execSQL(self.conn,
                       f'''CREATE TABLE {self.schema_name}.{self.table_rebalance_status_detail}
                       (db_name TEXT, schema_name TEXT, rel_name TEXT, status TEXT,
                       CONSTRAINT unique_fqn UNIQUE (db_name, schema_name, rel_name))
                       DISTRIBUTED REPLICATED''')
        dbconn.execSQL(self.conn,
                       f'''CREATE TABLE {self.schema_name}.{self.saved_plan}
                       (plan BYTEA)
                       DISTRIBUTED REPLICATED''')

        self.savePlan(plan)

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

    def addTableToRebalance(self, db: str, schema_name: str, rel_name: str, status: str) -> None:
        dbconn.execSQL(self.conn,
                       f'''INSERT INTO {self.schema_name}.{self.table_rebalance_status_detail}
                       VALUES ('{db}', '{schema_name}', '{rel_name}', '{status}')''')

    def setStatusForTableToRebalance(self, db: str, schema_name: str, rel_name: str, status: str) -> None:
        dbconn.execSQL(self.conn,
                       f'''UPDATE {self.schema_name}.{self.table_rebalance_status_detail} SET status = '{status}'
                       WHERE db_name = '{db}' AND schema_name = '{schema_name}' AND rel_name = '{rel_name}';''')

    def getTablesToRebalanceWithStatus(self, status: str) -> cursor:
        return dbconn.query(self.conn, f"""SELECT db_name, schema_name, rel_name FROM
                            {self.schema_name}.{self.table_rebalance_status_detail} WHERE status = '{status}'""")
