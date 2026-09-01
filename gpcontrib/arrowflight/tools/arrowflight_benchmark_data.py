#!/usr/bin/env python3
import argparse
import csv
import datetime as dt
import json
import math
from pathlib import Path


POSTGRES_EPOCH = dt.datetime(2000, 1, 1)
MIXED_NATTS = 7
FIXED_NATTS = 6


def bench_label(segid: int, row: int, width: int) -> str:
    label = f"bench-seg-{segid}-row-{row}"
    if len(label) < width:
        label += "x" * (width - len(label))
    return label


def csv_row(segid: int, row: int, rows_per_segment: int, label_width: int,
            schema: str):
    row_id = segid * rows_per_segment + row
    day = row % 365 + 1
    ts = POSTGRES_EPOCH + dt.timedelta(seconds=row)
    values = [
        row_id,
        segid,
        "t" if row % 2 == 1 else "f",
        f"{row + 0.5:.1f}",
        (POSTGRES_EPOCH.date() + dt.timedelta(days=day)).isoformat(),
        ts.strftime("%Y-%m-%d %H:%M:%S"),
    ]

    if schema == "mixed":
        values.insert(2, bench_label(segid, row, label_width))

    return values


def batch_count(rows_per_segment: int, batch_rows: int) -> int:
    return (rows_per_segment + batch_rows - 1) // batch_rows


def bitmap_bytes(rows_per_segment: int, batch_rows: int) -> int:
    total = 0
    remaining = rows_per_segment

    while remaining > 0:
        rows = min(remaining, batch_rows)
        total += math.ceil(rows / 8)
        remaining -= rows

    return total


def arrow_raw_bytes(rows_per_segment: int, label_bytes: int,
                    batch_rows: int, schema: str) -> int:
    batches = batch_count(rows_per_segment, batch_rows)

    # Benchmark schema:
    # int32, int32, utf8, bool, float64, date32, timestamp[us].
    fixed_width_bytes = rows_per_segment * (4 + 4 + 8 + 4 + 8)
    bool_bitmap_bytes = bitmap_bytes(rows_per_segment, batch_rows)

    if schema == "fixed":
        return fixed_width_bytes + bool_bitmap_bytes

    string_offsets_bytes = 4 * (rows_per_segment + batches)
    return fixed_width_bytes + string_offsets_bytes + label_bytes + \
        bool_bitmap_bytes


def write_segment(output_dir: Path, segid: int, rows_per_segment: int,
                  label_width: int, batch_rows: int, schema: str) -> dict:
    csv_path = output_dir / f"bench_{segid}.csv"
    id_sum = 0
    segid_sum = 0
    label_bytes = 0

    with csv_path.open("w", newline="", encoding="utf-8") as csv_file:
        writer = csv.writer(csv_file, lineterminator="\n")

        for row in range(1, rows_per_segment + 1):
            row_id = segid * rows_per_segment + row
            if schema == "mixed":
                label_bytes += len(bench_label(segid, row, label_width)
                                   .encode("utf-8"))
            id_sum += row_id
            segid_sum += segid
            writer.writerow(csv_row(segid, row, rows_per_segment,
                                    label_width, schema))

    raw_arrow_bytes = arrow_raw_bytes(rows_per_segment, label_bytes,
                                      batch_rows, schema)
    validity_bytes = bitmap_bytes(rows_per_segment, batch_rows) * (
        MIXED_NATTS if schema == "mixed" else FIXED_NATTS)

    return {
        "segid": segid,
        "rows": rows_per_segment,
        "csv_file": str(csv_path),
        "csv_bytes": csv_path.stat().st_size,
        "arrow_raw_bytes": raw_arrow_bytes,
        "arrow_raw_with_validity_bytes": raw_arrow_bytes + validity_bytes,
        "label_bytes": label_bytes,
        "id_sum": id_sum,
        "segid_sum": segid_sum,
    }


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate Arrow Flight benchmark CSV shards and metadata.")
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--segments", type=int, required=True)
    parser.add_argument("--rows-per-segment", type=int, required=True)
    parser.add_argument("--batch-rows", type=int, default=8192)
    parser.add_argument("--label-width", type=int, default=32)
    parser.add_argument("--schema", choices=("mixed", "fixed"),
                        default="mixed")
    args = parser.parse_args()

    if args.segments <= 0:
        raise SystemExit("--segments must be positive")
    if args.rows_per_segment <= 0:
        raise SystemExit("--rows-per-segment must be positive")
    if args.batch_rows <= 0:
        raise SystemExit("--batch-rows must be positive")
    if args.label_width <= 0:
        raise SystemExit("--label-width must be positive")

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    segments = [
        write_segment(output_dir, segid, args.rows_per_segment,
                      args.label_width, args.batch_rows, args.schema)
        for segid in range(args.segments)
    ]
    csv_bytes = sum(segment["csv_bytes"] for segment in segments)
    arrow_raw_bytes_total = sum(segment["arrow_raw_bytes"]
                                for segment in segments)
    arrow_raw_with_validity_bytes = sum(
        segment["arrow_raw_with_validity_bytes"] for segment in segments)
    rows = sum(segment["rows"] for segment in segments)
    id_sum = sum(segment["id_sum"] for segment in segments)
    segid_sum = sum(segment["segid_sum"] for segment in segments)

    metadata = {
        "segments": args.segments,
        "rows_per_segment": args.rows_per_segment,
        "batch_rows": args.batch_rows,
        "rows": rows,
        "schema": args.schema,
        "label_width": args.label_width,
        "csv_bytes": csv_bytes,
        "arrow_raw_bytes": arrow_raw_bytes_total,
        "arrow_raw_with_validity_bytes": arrow_raw_with_validity_bytes,
        "arrow_raw_bytes_kind": "estimated_arrow_array_buffers_no_ipc_or_grpc_framing",
        "logical_bytes": csv_bytes,
        "id_sum": id_sum,
        "segid_sum": segid_sum,
        "shards": segments,
    }

    metadata_path = output_dir / "metadata.json"
    metadata_path.write_text(json.dumps(metadata, indent=2) + "\n",
                             encoding="utf-8")
    print(json.dumps(metadata, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
