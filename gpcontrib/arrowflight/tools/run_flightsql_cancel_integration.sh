#!/usr/bin/env bash
set -euo pipefail

# Run in the synthetic Compose Greengage container (QD + three QEs).
export FLIGHTSQL_DELAY_HOST="${FLIGHTSQL_DELAY_HOST:-flightsql-synthetic-delay}"
export FLIGHTSQL_DELAY_PORT="${FLIGHTSQL_DELAY_PORT:-9018}"
python3 - <<'PY'
import os
import socket
import socketserver
import threading
import time

import psycopg2
from psycopg2 import sql


class Blackhole(socketserver.BaseRequestHandler):
    def handle(self):
        self.request.settimeout(0.1)
        while not stopping.is_set():
            try:
                if not self.request.recv(65536):
                    return
            except socket.timeout:
                continue


stopping = threading.Event()
blackhole = socketserver.ThreadingTCPServer(('0.0.0.0', 19090), Blackhole)
blackhole.daemon_threads = True
listener = threading.Thread(target=blackhole.serve_forever)
listener.start()
control = psycopg2.connect(dbname='postgres')
control.autocommit = True
name = f'flightsql_cancel_{os.getpid()}'


def execute(query, *args):
    with control.cursor() as cursor:
        cursor.execute(query, *args)


try:
    execute('CREATE EXTENSION IF NOT EXISTS arrowflight')
    execute(sql.SQL('CREATE SCHEMA {}').format(sql.Identifier(name)))
    execute(sql.SQL('''CREATE SERVER {} FOREIGN DATA WRAPPER flightsql_fdw
        OPTIONS (host {}, port {}, timeout_ms '0', read_timeout_ms '0',
                 endpoint_location_allowlist 'grpc+tcp://greengage:19090')''').format(
        sql.Identifier(name), sql.Literal(os.environ['FLIGHTSQL_DELAY_HOST']),
        sql.Literal(os.environ['FLIGHTSQL_DELAY_PORT'])))
    execute(sql.SQL('''CREATE SERVER {} FOREIGN DATA WRAPPER flightsql_fdw
        OPTIONS (host '127.0.0.1', port '19090', timeout_ms '0')''').format(
        sql.Identifier(name + '_blackhole')))
    phases = {
        'execute': 'af_perf_read_slow_execute',
        'schema': 'af_perf_read_slow_schema',
        'batch': 'af_perf_read_slow_batch',
        'connect_doget': 'af_perf_read_blackhole',
        'connect_execute': 'af_perf_read',
    }
    for phase, remote in phases.items():
        server = name + '_blackhole' if phase == 'connect_execute' else name
        execute(sql.SQL('''CREATE FOREIGN TABLE {}.{} (id int4)
            SERVER {} OPTIONS (table_name {})''').format(
            sql.Identifier(name), sql.Identifier(phase), sql.Identifier(server),
            sql.Literal(remote)))

    for optimizer in ('on', 'off'):
        for phase in phases:
            for action in ('timeout', 'cancel', 'terminate'):
                connection = psycopg2.connect(dbname='postgres')
                connection.autocommit = True
                with connection.cursor() as cursor:
                    cursor.execute(f'SET optimizer = {optimizer}')
                    cursor.execute('SET statement_timeout = 500' if action == 'timeout'
                                   else 'SET statement_timeout = 15000')
                result = []

                def read():
                    try:
                        with connection.cursor() as cursor:
                            cursor.execute(sql.SQL('SELECT count(*) FROM {}.{}').format(
                                sql.Identifier(name), sql.Identifier(phase)))
                        result.append('unexpected success')
                    except psycopg2.Error as error:
                        result.append(error.pgcode)

                worker = threading.Thread(target=read)
                started = time.monotonic()
                worker.start()
                try:
                    if action != 'timeout':
                        time.sleep(0.5)
                        with control.cursor() as cursor:
                            function = 'pg_cancel_backend' if action == 'cancel' else 'pg_terminate_backend'
                            cursor.execute(f'SELECT {function}(%s)', (connection.get_backend_pid(),))
                            assert cursor.fetchone() == (True,)
                    worker.join(3)
                    elapsed = time.monotonic() - started
                    assert not worker.is_alive(), (phase, action, 'RPC still blocked')
                    assert elapsed < 3, (phase, action, elapsed)
                    if action == 'terminate':
                        # libpq may discard the FATAL SQLSTATE after connection loss.
                        assert result in (['57P01'], [None]), result
                        assert connection.closed, 'terminated connection is still open'
                    else:
                        assert result == ['57014'], (phase, action, result)
                        with connection.cursor() as cursor:
                            cursor.execute('SELECT 1')
                            assert cursor.fetchone() == (1,)
                    print(f'PASS optimizer={optimizer} phase={phase} action={action} elapsed={elapsed:.3f}s', flush=True)
                finally:
                    if worker.is_alive():
                        execute('SELECT pg_terminate_backend(%s)', (connection.get_backend_pid(),))
                    worker.join(20)
                    connection.close()
finally:
    execute(sql.SQL('DROP SCHEMA IF EXISTS {} CASCADE').format(sql.Identifier(name)))
    execute(sql.SQL('DROP SERVER IF EXISTS {}').format(sql.Identifier(name)))
    execute(sql.SQL('DROP SERVER IF EXISTS {}').format(sql.Identifier(name + '_blackhole')))
    control.close()
    stopping.set()
    blackhole.shutdown()
    listener.join()
    blackhole.server_close()
PY
