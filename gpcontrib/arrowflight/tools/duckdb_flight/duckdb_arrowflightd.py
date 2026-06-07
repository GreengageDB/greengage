#!/usr/bin/env python3
"""DuckDB-backed Arrow Flight service for Greengage integration tests."""

from __future__ import annotations

import argparse
import json
import re
import sys
import threading
from dataclasses import dataclass, field
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Iterable
from urllib.parse import unquote

import duckdb
import pyarrow as pa
import pyarrow.flight as flight


DATASET_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
OPERATION_RE = re.compile(r"^[A-Za-z0-9_.-]+$")
LOG_LOCK = threading.Lock()


def _log(message: str) -> None:
    with LOG_LOCK:
        print(message, flush=True)


def _decode_part(part: object) -> str:
    if isinstance(part, bytes):
        return part.decode("utf-8")
    return str(part)


def _descriptor_parts(descriptor: flight.FlightDescriptor) -> list[str]:
    parts = [_decode_part(part) for part in descriptor.path]
    if len(parts) == 1 and "/" in parts[0]:
        return [part for part in parts[0].split("/") if part]
    return parts


def _ticket_parts(ticket: flight.Ticket) -> list[str]:
    raw = ticket.ticket
    if isinstance(raw, bytes):
        text = raw.decode("utf-8")
    else:
        text = str(raw)
    return [part for part in text.split("/") if part]


def _schema_metadata(schema: pa.Schema) -> dict[str, str]:
    if schema.metadata is None:
        return {}
    return {
        key.decode("utf-8"): value.decode("utf-8")
        for key, value in schema.metadata.items()
    }


def _validate_identifier(value: str, kind: str) -> None:
    pattern = DATASET_RE if kind == "dataset" else OPERATION_RE
    if not value or not pattern.match(value):
        raise flight.FlightServerError(f"invalid {kind}: {value!r}")


def _quote_ident(name: str) -> str:
    _validate_identifier(name, "dataset")
    return '"' + name.replace('"', '""') + '"'


def _quote_sql_identifier(name: str) -> str:
    if not name or any(ord(ch) < 0x20 or ord(ch) == 0x7F for ch in name):
        raise flight.FlightServerError(f"invalid SQL identifier: {name!r}")
    return '"' + name.replace('"', '""') + '"'


def _decode_projection_columns(encoded: str) -> list[str]:
    columns = [unquote(part) for part in encoded.split(",") if part]
    if not columns:
        raise flight.FlightServerError("empty projection column list")
    if len(set(columns)) != len(columns):
        raise flight.FlightServerError("duplicate projection column")
    return columns


def _flight_info_descriptor(
    dataset: str,
    segments: int,
    encoded_columns: str | None = None,
) -> flight.FlightDescriptor:
    path = f"dataset/{dataset}/segments/{segments}"
    if encoded_columns:
        path += f"/columns/{encoded_columns}"
    return flight.FlightDescriptor.for_path(path)


@dataclass
class SegmentWrite:
    table: pa.Table
    rows: int
    batches: int
    finalized: bool = False


@dataclass
class OperationWrite:
    dataset: str
    expected_segments: int
    segments: dict[int, SegmentWrite] = field(default_factory=dict)
    aborted: bool = False
    committed: bool = False


class DuckDbFlightServer(flight.FlightServerBase):
    def __init__(self, host: str, port: int) -> None:
        self._location = flight.Location.for_grpc_tcp(host, port)
        super().__init__(self._location)
        self._lock = threading.RLock()
        self._con = duckdb.connect(database=":memory:")
        self._operations: dict[str, OperationWrite] = {}
        self._init_schema()

    def _init_schema(self) -> None:
        self._con.execute(
            """
            CREATE TABLE sales (
                id INTEGER,
                segid INTEGER,
                label VARCHAR,
                active BOOLEAN,
                amount DOUBLE,
                d DATE,
                ts TIMESTAMP
            )
            """
        )
        rows = []
        for segid in range(2):
            for row in range(1, 5):
                rows.append(
                    (
                        segid * 100 + row,
                        segid,
                        f"duckdb-seg-{segid}-row-{row}",
                        row % 2 == 1,
                        float(row) + 0.5,
                        f"2026-06-{7 + row:02d}",
                        f"2026-06-07 12:00:0{row}",
                    )
                )
        self._con.executemany("INSERT INTO sales VALUES (?, ?, ?, ?, ?, ?, ?)", rows)

    def query_json(self, sql: str) -> dict[str, object]:
        if not sql.strip():
            raise ValueError("SQL must not be empty")

        with self._lock:
            result = self._con.execute(sql)
            if result.description is None:
                return {"columns": [], "rows": [], "row_count": 0}
            columns = [item[0] for item in result.description]
            rows = result.fetchall()
            return {
                "columns": columns,
                "rows": rows,
                "row_count": len(rows),
            }

    def get_flight_info(
        self,
        context: flight.ServerCallContext,
        descriptor: flight.FlightDescriptor,
    ) -> flight.FlightInfo:
        del context
        parts = _descriptor_parts(descriptor)
        if (
            len(parts) not in {4, 6}
            or parts[0] != "dataset"
            or parts[2] != "segments"
            or (len(parts) == 6 and parts[4] != "columns")
        ):
            raise flight.FlightServerError(f"unknown descriptor: {parts}")

        dataset = parts[1]
        _validate_identifier(dataset, "dataset")
        segments = int(parts[3])
        if segments <= 0:
            raise flight.FlightServerError(f"invalid segment count: {segments}")
        encoded_columns = parts[5] if len(parts) == 6 else None
        columns = _decode_projection_columns(encoded_columns) if encoded_columns else None

        endpoints = [
            flight.FlightEndpoint(
                flight.Ticket(
                    (
                        f"af-v1/dataset/{dataset}/segment/{segid}"
                        + (f"/columns/{encoded_columns}" if encoded_columns else "")
                    ).encode()
                ),
                [],
            )
            for segid in range(segments)
        ]
        schema = self._read_dataset_segment(dataset, 0, columns).schema
        total_records = self._count_dataset(dataset)
        return flight.FlightInfo(
            schema,
            _flight_info_descriptor(dataset, segments, encoded_columns),
            endpoints,
            total_records,
            -1,
        )

    def do_get(
        self,
        context: flight.ServerCallContext,
        ticket: flight.Ticket,
    ) -> flight.RecordBatchStream:
        del context
        parts = _ticket_parts(ticket)
        if (
            len(parts) not in {5, 7}
            or parts[0] != "af-v1"
            or parts[1] != "dataset"
            or parts[3] != "segment"
            or (len(parts) == 7 and parts[5] != "columns")
        ):
            raise flight.FlightServerError(f"unknown ticket: {parts}")

        dataset = parts[2]
        segid = int(parts[4])
        encoded_columns = parts[6] if len(parts) == 7 else None
        columns = _decode_projection_columns(encoded_columns) if encoded_columns else None
        table = self._read_dataset_segment(dataset, segid, columns)
        column_log = ",".join(columns) if columns else "*"
        _log(
            "duckdb_flight_do_get "
            f"dataset={dataset} segment={segid} columns={column_log} rows={table.num_rows}"
        )
        return flight.RecordBatchStream(table)

    def do_put(
        self,
        context: flight.ServerCallContext,
        descriptor: flight.FlightDescriptor,
        reader: flight.FlightMetadataReader,
        writer: flight.FlightMetadataWriter,
    ) -> None:
        del context
        parts = _descriptor_parts(descriptor)
        if len(parts) != 6 or parts[0] != "af-v1" or parts[1] != "write" or parts[4] != "segment":
            raise flight.FlightServerError(f"unknown write descriptor: {parts}")

        dataset = parts[2]
        operation_id = parts[3]
        segid = int(parts[5])
        _validate_identifier(dataset, "dataset")
        _validate_identifier(operation_id, "operation")

        table = reader.read_all()
        metadata = _schema_metadata(table.schema)
        expected_segments = int(metadata.get("af.segment.count", "0"))
        if expected_segments <= 0:
            raise flight.FlightServerError("missing or invalid af.segment.count")

        batches = len(table.to_batches())
        self._record_segment_write(dataset, operation_id, segid, expected_segments, table, batches)
        self._write_metadata(
            writer,
            (
                "af.ack.final=true\n"
                f"af.ack.rows={table.num_rows}\n"
                f"af.ack.batches={batches}\n"
            ).encode(),
        )
        _log(
            "duckdb_flight_do_put "
            f"dataset={dataset} operation_id={operation_id} segment={segid} "
            f"rows={table.num_rows} batches={batches}"
        )

    def do_action(
        self,
        context: flight.ServerCallContext,
        action: flight.Action,
    ) -> Iterable[bytes]:
        del context
        if action.type not in {"FinalizeOperation", "AbortOperation"}:
            raise flight.FlightServerError(f"unknown action: {action.type}")

        values = self._parse_action_body(action.body)
        operation_id = values.get("af.operation.id", "")
        dataset = values.get("af.dataset", "")
        segid = int(values.get("af.segment.index", "-1"))
        expected_segments = int(values.get("af.segment.count", "0"))
        _validate_identifier(dataset, "dataset")
        _validate_identifier(operation_id, "operation")

        with self._lock:
            operation = self._operations.get(operation_id)
            if operation is None:
                raise flight.FlightServerError(f"unknown operation: {operation_id}")
            if expected_segments > operation.expected_segments:
                operation.expected_segments = expected_segments

            if action.type == "AbortOperation":
                operation.aborted = True
                state = "aborted"
            else:
                segment = operation.segments.get(segid)
                if segment is None:
                    raise flight.FlightServerError(f"unknown operation segment: {operation_id}/{segid}")
                segment.finalized = True
                finalized = sum(1 for item in operation.segments.values() if item.finalized)
                if (
                    not operation.aborted
                    and not operation.committed
                    and operation.expected_segments > 0
                    and finalized >= operation.expected_segments
                ):
                    self._commit_operation_locked(operation)
                    state = "complete"
                else:
                    state = "partial"

        _log(
            "duckdb_flight_action "
            f"action={action.type} dataset={dataset} operation_id={operation_id} "
            f"segment={segid} state={state}"
        )
        yield (
            "af.action.ok=true\n"
            f"af.action.type={action.type}\n"
            f"af.operation.id={operation_id}\n"
            f"af.finalize.state={state}\n"
        ).encode()

    def list_actions(self, context: flight.ServerCallContext) -> list[tuple[str, str]]:
        del context
        return [
            ("FinalizeOperation", "Commit a staged DuckDB write operation"),
            ("AbortOperation", "Abort a staged DuckDB write operation"),
        ]

    def _read_dataset_segment(
        self,
        dataset: str,
        segid: int,
        columns: list[str] | None = None,
    ) -> pa.Table:
        _validate_identifier(dataset, "dataset")
        with self._lock:
            table_name = _quote_ident(dataset)
            if not self._table_exists_locked(dataset):
                raise flight.FlightServerError(f"unknown dataset: {dataset}")
            if columns is None:
                select_list = "id, segid, label, active, amount, d, ts"
            else:
                self._validate_columns_locked(dataset, columns)
                select_list = ", ".join(_quote_sql_identifier(column) for column in columns)
            return self._con.execute(
                f"SELECT {select_list} "
                f"FROM {table_name} WHERE segid = ? ORDER BY id",
                [segid],
            ).fetch_arrow_table()

    def _count_dataset(self, dataset: str) -> int:
        _validate_identifier(dataset, "dataset")
        with self._lock:
            table_name = _quote_ident(dataset)
            if not self._table_exists_locked(dataset):
                raise flight.FlightServerError(f"unknown dataset: {dataset}")
            return int(self._con.execute(f"SELECT count(*) FROM {table_name}").fetchone()[0])

    def _table_exists_locked(self, dataset: str) -> bool:
        return (
            self._con.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_name = ?",
                [dataset],
            ).fetchone()[0]
            > 0
        )

    def _validate_columns_locked(self, dataset: str, columns: list[str]) -> None:
        available = {
            row[0]
            for row in self._con.execute(
                "SELECT column_name FROM information_schema.columns WHERE table_name = ?",
                [dataset],
            ).fetchall()
        }
        for column in columns:
            if column not in available:
                raise flight.FlightServerError(f"unknown projection column: {column}")

    def _record_segment_write(
        self,
        dataset: str,
        operation_id: str,
        segid: int,
        expected_segments: int,
        table: pa.Table,
        batches: int,
    ) -> None:
        with self._lock:
            operation = self._operations.setdefault(
                operation_id,
                OperationWrite(dataset=dataset, expected_segments=expected_segments),
            )
            if operation.dataset != dataset:
                raise flight.FlightServerError("operation dataset changed")
            if expected_segments > operation.expected_segments:
                operation.expected_segments = expected_segments
            operation.segments[segid] = SegmentWrite(table=table, rows=table.num_rows, batches=batches)

    def _commit_operation_locked(self, operation: OperationWrite) -> None:
        if operation.aborted or operation.committed:
            return
        if not operation.segments:
            raise flight.FlightServerError("operation has no segment data")

        combined = pa.concat_tables(
            [segment.table for _, segment in sorted(operation.segments.items())],
            promote_options="default",
        )
        dataset = operation.dataset
        table_name = _quote_ident(dataset)
        self._con.register("af_incoming", combined)
        try:
            if not self._table_exists_locked(dataset):
                self._con.execute(f"CREATE TABLE {table_name} AS SELECT * FROM af_incoming WHERE false")
            self._con.execute(f"INSERT INTO {table_name} SELECT * FROM af_incoming")
        finally:
            self._con.unregister("af_incoming")
        operation.committed = True

    @staticmethod
    def _parse_action_body(body: pa.Buffer | bytes | None) -> dict[str, str]:
        if body is None:
            return {}
        if isinstance(body, pa.Buffer):
            text = body.to_pybytes().decode("utf-8")
        elif isinstance(body, bytes):
            text = body.decode("utf-8")
        else:
            text = bytes(body).decode("utf-8")

        values: dict[str, str] = {}
        for line in text.splitlines():
            if "=" in line:
                key, value = line.split("=", 1)
                values[key] = value
        return values

    @staticmethod
    def _write_metadata(writer: flight.FlightMetadataWriter, payload: bytes) -> None:
        if hasattr(writer, "write"):
            writer.write(pa.py_buffer(payload))
            return
        if hasattr(writer, "write_metadata"):
            writer.write_metadata(pa.py_buffer(payload))
            return
        raise flight.FlightServerError("Flight metadata writer has no write method")


def main(argv: list[str]) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8815)
    parser.add_argument("--admin-host", default="0.0.0.0")
    parser.add_argument("--admin-port", type=int, default=8816)
    args = parser.parse_args(argv)

    server = DuckDbFlightServer(args.host, args.port)
    admin_httpd = make_admin_httpd(server, args.admin_host, args.admin_port)
    threading.Thread(target=admin_httpd.serve_forever, daemon=True).start()
    _log(f"duckdb_arrowflightd listening on {args.host}:{args.port}")
    _log(f"duckdb_arrowflightd_admin listening on {args.admin_host}:{args.admin_port}")
    server.serve()
    return 0


def make_admin_httpd(
    server: DuckDbFlightServer,
    host: str,
    port: int,
) -> ThreadingHTTPServer:
    class AdminHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            if self.path != "/health":
                self.send_error(404)
                return
            self._send_json({"ok": True})

        def do_POST(self) -> None:
            if self.path != "/query":
                self.send_error(404)
                return

            try:
                length = int(self.headers.get("Content-Length", "0"))
                body = self.rfile.read(length).decode("utf-8")
                content_type = self.headers.get("Content-Type", "")
                if "application/json" in content_type:
                    payload = json.loads(body)
                    sql = str(payload.get("sql", ""))
                else:
                    sql = body
                self._send_json(server.query_json(sql))
            except Exception as exc:
                self._send_json({"error": str(exc)}, status=400)

        def log_message(self, fmt: str, *args: object) -> None:
            _log("duckdb_admin_http " + (fmt % args))

        def _send_json(self, payload: dict[str, object], status: int = 200) -> None:
            encoded = json.dumps(payload, default=str).encode("utf-8")
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

    return ThreadingHTTPServer((host, port), AdminHandler)


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
