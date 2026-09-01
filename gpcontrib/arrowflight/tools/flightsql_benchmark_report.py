#!/usr/bin/env python3
import argparse
import csv
import json
import math
import statistics
from pathlib import Path


MIB = 1024 ** 2
GIB = 1024 ** 3


def percentile(values: list[float], fraction: float) -> float:
    ordered = sorted(values)
    return ordered[max(0, math.ceil(len(ordered) * fraction) - 1)]


def load_runs(manifest_path: Path) -> list[dict]:
    runs = []
    with manifest_path.open(newline="", encoding="utf-8") as source:
        for row in csv.DictReader(source):
            metrics = json.loads(
                Path(row["metrics_file"]).read_text(encoding="utf-8"))
            row["metrics"] = metrics
            runs.append(row)
    return runs


def sum_group(
    containers: dict[str, dict],
    aliases: list[str],
    field: str,
) -> float:
    return sum(containers[alias][field] for alias in aliases)


def summarize(
    runs: list[dict],
    rows: int,
    logical_bytes: int,
) -> list[dict]:
    summaries = []
    methods = []
    for run in runs:
        key = (run["workload"], run["method"])
        if run["phase"] == "run" and key not in methods:
            methods.append(key)

    logical_gib = logical_bytes / GIB
    for workload, method in methods:
        selected = [
            run for run in runs
            if run["phase"] == "run"
            and run["workload"] == workload
            and run["method"] == method
        ]
        if not selected:
            continue

        normalized = []
        for run in selected:
            metrics = run["metrics"]
            containers = metrics["containers"]
            remote_aliases = [
                alias for alias in containers if alias != "greengage"
            ]
            wall_sec = metrics["wall_sec"]
            gg_cpu_sec = containers["greengage"]["cpu_sec"]
            remote_cpu_sec = sum_group(
                containers, remote_aliases, "cpu_sec")
            total_cpu_sec = gg_cpu_sec + remote_cpu_sec
            gg_memory_delta = containers["greengage"][
                "peak_memory_delta_bytes"]
            remote_memory_delta = sum_group(
                containers, remote_aliases, "peak_memory_delta_bytes")
            gg_peak_memory = containers["greengage"]["peak_memory_bytes"]
            remote_peak_memory = sum_group(
                containers, remote_aliases, "peak_memory_bytes")
            gg_network_bytes = (
                containers["greengage"]["rx_bytes"]
                + containers["greengage"]["tx_bytes"]
            )
            remote_network_bytes = sum(
                containers[alias]["rx_bytes"] + containers[alias]["tx_bytes"]
                for alias in remote_aliases
            )
            control_network_bytes = 0
            if "flightsql_mpp_control" in containers:
                control = containers["flightsql_mpp_control"]
                control_network_bytes = (
                    control["rx_bytes"] + control["tx_bytes"]
                )
            normalized.append({
                "run": int(run["run"]),
                "wall_sec": wall_sec,
                "rows_per_sec": rows / wall_sec,
                "gg_cpu_sec": gg_cpu_sec,
                "remote_cpu_sec": remote_cpu_sec,
                "total_cpu_sec": total_cpu_sec,
                "cpu_sec_per_logical_gib": (
                    total_cpu_sec / logical_gib if logical_gib else 0
                ),
                "gg_peak_memory_delta_bytes": gg_memory_delta,
                "remote_peak_memory_delta_bytes": remote_memory_delta,
                "total_peak_memory_delta_bytes": (
                    metrics["aggregate"]["peak_memory_delta_bytes"]
                ),
                "gg_peak_memory_bytes": gg_peak_memory,
                "remote_peak_memory_bytes": remote_peak_memory,
                "gg_network_bytes": gg_network_bytes,
                "gg_network_bytes_per_row": gg_network_bytes / rows,
                "remote_network_bytes": remote_network_bytes,
                "control_network_bytes": control_network_bytes,
                "sample_count": metrics["sample_count"],
            })

        def median(field: str) -> float:
            return statistics.median(row[field] for row in normalized)

        wall_values = [row["wall_sec"] for row in normalized]
        summaries.append({
            "workload": workload,
            "method": method,
            "runs": len(normalized),
            "median_wall_sec": median("wall_sec"),
            "p95_wall_sec": percentile(wall_values, 0.95),
            "median_rows_per_sec": median("rows_per_sec"),
            "median_gg_cpu_sec": median("gg_cpu_sec"),
            "median_remote_cpu_sec": median("remote_cpu_sec"),
            "median_total_cpu_sec": median("total_cpu_sec"),
            "median_cpu_sec_per_logical_gib": median(
                "cpu_sec_per_logical_gib"),
            "median_gg_peak_memory_delta_mib": (
                median("gg_peak_memory_delta_bytes") / MIB
            ),
            "median_remote_peak_memory_delta_mib": (
                median("remote_peak_memory_delta_bytes") / MIB
            ),
            "median_total_peak_memory_delta_mib": (
                median("total_peak_memory_delta_bytes") / MIB
            ),
            "median_gg_peak_memory_mib": (
                median("gg_peak_memory_bytes") / MIB
            ),
            "median_remote_peak_memory_mib": (
                median("remote_peak_memory_bytes") / MIB
            ),
            "median_gg_network_mib": median("gg_network_bytes") / MIB,
            "median_gg_network_bytes_per_row": median(
                "gg_network_bytes_per_row"),
            "median_remote_network_mib": (
                median("remote_network_bytes") / MIB
            ),
            "median_control_network_mib": (
                median("control_network_bytes") / MIB
            ),
            "raw_runs": normalized,
        })

    return summaries


def write_markdown(
    path: Path,
    metadata: dict,
    config: dict,
    summaries: list[dict],
) -> None:
    lines = [
        "# Arrow Flight SQL Benchmark",
        "",
        (
            f"Dataset: {metadata['rows']:,} rows, "
            f"{metadata['segments']} Greengage segments, "
            f"{metadata['rows_per_segment']:,} rows per segment, "
            f"{metadata['logical_bytes'] / MIB:.2f} MiB logical CSV size."
        ),
        "",
        (
            f"Runs: {config['warmups']} warmup and "
            f"{config['repeats']} measured; "
            f"resource sampling interval "
            f"{config['sample_interval_sec']:.3f} seconds."
        ),
        "",
        (
            "| workload | method | rows/s | wall p50 s | wall p95 s | "
            "GG CPU s | remote CPU s | total CPU s | CPU s/GiB | "
            "GG net MiB | remote net MiB | control net MiB | GG net B/row |"
        ),
        (
            "|---|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|"
        ),
    ]
    for row in summaries:
        lines.append(
            f"| {row['workload']} | {row['method']} | "
            f"{row['median_rows_per_sec']:,.0f} | "
            f"{row['median_wall_sec']:.3f} | "
            f"{row['p95_wall_sec']:.3f} | "
            f"{row['median_gg_cpu_sec']:.3f} | "
            f"{row['median_remote_cpu_sec']:.3f} | "
            f"{row['median_total_cpu_sec']:.3f} | "
            f"{row['median_cpu_sec_per_logical_gib']:.2f} | "
            f"{row['median_gg_network_mib']:.2f} | "
            f"{row['median_remote_network_mib']:.2f} | "
            f"{row['median_control_network_mib']:.2f} | "
            f"{row['median_gg_network_bytes_per_row']:.2f} |"
        )

    lines.extend([
        "",
        (
            "| workload | method | GG RAM peak MiB | GG RAM delta MiB | "
            "remote RAM peak MiB | remote RAM delta MiB |"
        ),
        "|---|---|---:|---:|---:|---:|",
    ])
    for row in summaries:
        lines.append(
            f"| {row['workload']} | {row['method']} | "
            f"{row['median_gg_peak_memory_mib']:.2f} | "
            f"{row['median_gg_peak_memory_delta_mib']:.2f} | "
            f"{row['median_remote_peak_memory_mib']:.2f} | "
            f"{row['median_remote_peak_memory_delta_mib']:.2f} |"
        )

    lines.extend([
        "",
        "Measurement boundaries:",
        "",
        "- Greengage CPU and RAM include the coordinator and all segment processes.",
        "- Remote CPU, RAM, and network include gpfdist, the synthetic origin service, all planned control/worker processes, or both ClickHouse shards.",
        "- Control net is reported separately for planned writes; it should contain only plan and transaction RPC traffic, not Arrow batches.",
        "- RAM peak is sampled cgroup memory usage, including page cache; RAM delta is its increase above the pre-query value.",
        "- GG network is the Greengage container eth0 rx+tx delta.",
        "- Flight SQL includes ClickHouse SQL execution and distributed-table work.",
        "- Synthetic Flight SQL isolates protocol overhead by serving prebuilt Arrow IPC data without a SQL engine.",
        "- ClickHouse is restarted between the read and write phases so read-side allocator and page-cache state does not affect write measurements.",
        "- ClickHouse releases each ticket after its successful DoGet.",
        "- The ClickHouse read view materializes `segid` to avoid ClickHouse 26.4 Flight SQL corruption of block-constant Int32 columns.",
        "",
    ])
    path.write_text("\n".join(lines), encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manifest", required=True)
    parser.add_argument("--metadata", required=True)
    parser.add_argument("--config", required=True)
    parser.add_argument("--output-json", required=True)
    parser.add_argument("--output-markdown", required=True)
    args = parser.parse_args()

    manifest_path = Path(args.manifest)
    metadata = json.loads(Path(args.metadata).read_text(encoding="utf-8"))
    config = json.loads(Path(args.config).read_text(encoding="utf-8"))
    runs = load_runs(manifest_path)
    summaries = summarize(
        runs,
        int(metadata["rows"]),
        int(metadata["logical_bytes"]),
    )

    result = {
        "config": config,
        "dataset": metadata,
        "methods": summaries,
    }
    Path(args.output_json).write_text(
        json.dumps(result, indent=2) + "\n",
        encoding="utf-8",
    )
    markdown_path = Path(args.output_markdown)
    write_markdown(markdown_path, metadata, config, summaries)
    print(markdown_path.read_text(encoding="utf-8"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
