#!/usr/bin/env python3
import argparse
import http.client
import json
import os
import socket
import subprocess
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import quote


def parse_container(value: str) -> tuple[str, str]:
    if "=" not in value:
        raise argparse.ArgumentTypeError(
            "container must use the alias=container-name form")
    alias, container = value.split("=", 1)
    if not alias or not container:
        raise argparse.ArgumentTypeError(
            "container must use the alias=container-name form")
    return alias, container


def docker_socket_path(explicit: str | None) -> str:
    if explicit:
        return explicit

    docker_host = os.environ.get("DOCKER_HOST", "")
    if not docker_host:
        docker_host = subprocess.check_output(
            [
                "docker",
                "context",
                "inspect",
                "--format",
                "{{.Endpoints.docker.Host}}",
            ],
            text=True,
        ).strip()
    if not docker_host.startswith("unix://"):
        raise RuntimeError(
            "resource probe currently requires a unix Docker Engine socket")
    return docker_host.removeprefix("unix://")


class UnixHTTPConnection(http.client.HTTPConnection):
    def __init__(self, socket_path: str):
        super().__init__("localhost")
        self.socket_path = socket_path

    def connect(self) -> None:
        self.sock = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.sock.connect(self.socket_path)


def read_metrics(container: str, socket_path: str) -> dict[str, int]:
    connection = UnixHTTPConnection(socket_path)
    try:
        path = (
            f"/containers/{quote(container, safe='')}/stats"
            "?stream=false&one-shot=true"
        )
        connection.request("GET", path)
        response = connection.getresponse()
        payload = response.read()
        if response.status != 200:
            raise RuntimeError(
                f"Docker stats for {container} returned {response.status}: "
                f"{payload.decode('utf-8', errors='replace')}")
        stats = json.loads(payload)
    finally:
        connection.close()

    cpu_nanoseconds = stats["cpu_stats"]["cpu_usage"]["total_usage"]
    memory_bytes = stats["memory_stats"].get("usage", 0)
    networks = stats.get("networks", {})
    rx_bytes = sum(network.get("rx_bytes", 0)
                   for network in networks.values())
    tx_bytes = sum(network.get("tx_bytes", 0)
                   for network in networks.values())
    return {
        "cpu_usec": cpu_nanoseconds // 1000,
        "memory_bytes": memory_bytes,
        "rx_bytes": rx_bytes,
        "tx_bytes": tx_bytes,
    }


def read_all(
    containers: list[tuple[str, str]],
    executor: ThreadPoolExecutor,
    socket_path: str,
) -> dict[str, dict[str, int]]:
    futures = {
        alias: executor.submit(read_metrics, container, socket_path)
        for alias, container in containers
    }
    return {alias: future.result() for alias, future in futures.items()}


def delta(after: int, before: int) -> int:
    return max(after - before, 0)


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run a command and measure Docker container resources.")
    parser.add_argument(
        "--container",
        action="append",
        required=True,
        type=parse_container,
        dest="containers",
        help="Container to measure in alias=container-name form.",
    )
    parser.add_argument("--output", required=True)
    parser.add_argument("--interval", type=float, default=0.2)
    parser.add_argument("--docker-socket")
    parser.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()

    if args.interval <= 0:
        parser.error("--interval must be positive")
    if not args.command:
        parser.error("a command is required after --")
    if args.command[0] == "--":
        args.command = args.command[1:]
    if not args.command:
        parser.error("a command is required after --")

    aliases = [alias for alias, _ in args.containers]
    if len(aliases) != len(set(aliases)):
        parser.error("container aliases must be unique")

    socket_path = docker_socket_path(args.docker_socket)
    samples = []
    with ThreadPoolExecutor(max_workers=len(args.containers)) as executor:
        baseline = read_all(args.containers, executor, socket_path)
        started = time.monotonic()
        process = subprocess.Popen(
            args.command,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
        )

        while process.poll() is None:
            captured_at = time.monotonic()
            sample = read_all(args.containers, executor, socket_path)
            samples.append({
                "elapsed_sec": captured_at - started,
                "containers": sample,
            })
            remaining = args.interval - (time.monotonic() - captured_at)
            if remaining > 0:
                time.sleep(remaining)

        stdout, stderr = process.communicate()
        finished = time.monotonic()
        final = read_all(args.containers, executor, socket_path)
        samples.append({
            "elapsed_sec": finished - started,
            "containers": final,
        })

    container_results = {}
    for alias, container in args.containers:
        memory_values = [
            baseline[alias]["memory_bytes"],
            *(sample["containers"][alias]["memory_bytes"]
              for sample in samples),
        ]
        peak_memory = max(memory_values)
        container_results[alias] = {
            "container": container,
            "cpu_sec": delta(
                final[alias]["cpu_usec"],
                baseline[alias]["cpu_usec"],
            ) / 1_000_000,
            "baseline_memory_bytes": baseline[alias]["memory_bytes"],
            "peak_memory_bytes": peak_memory,
            "peak_memory_delta_bytes": delta(
                peak_memory,
                baseline[alias]["memory_bytes"],
            ),
            "rx_bytes": delta(
                final[alias]["rx_bytes"],
                baseline[alias]["rx_bytes"],
            ),
            "tx_bytes": delta(
                final[alias]["tx_bytes"],
                baseline[alias]["tx_bytes"],
            ),
        }

    aggregate_baseline = sum(
        metrics["memory_bytes"] for metrics in baseline.values())
    aggregate_samples = [
        sum(metrics["memory_bytes"]
            for metrics in sample["containers"].values())
        for sample in samples
    ]
    aggregate_peak = max([aggregate_baseline, *aggregate_samples])

    output = {
        "wall_sec": finished - started,
        "sample_interval_sec": args.interval,
        "sample_count": len(samples),
        "aggregate": {
            "cpu_sec": sum(
                metrics["cpu_sec"] for metrics in container_results.values()),
            "baseline_memory_bytes": aggregate_baseline,
            "peak_memory_bytes": aggregate_peak,
            "peak_memory_delta_bytes": delta(
                aggregate_peak,
                aggregate_baseline,
            ),
            "rx_bytes": sum(
                metrics["rx_bytes"] for metrics in container_results.values()),
            "tx_bytes": sum(
                metrics["tx_bytes"] for metrics in container_results.values()),
        },
        "containers": container_results,
        "command": {
            "argv": args.command,
            "returncode": process.returncode,
            "stdout": stdout,
            "stderr": stderr,
        },
    }

    output_path = Path(args.output)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(
        json.dumps(output, indent=2) + "\n",
        encoding="utf-8",
    )

    sys.stdout.write(stdout)
    sys.stderr.write(stderr)
    return process.returncode


if __name__ == "__main__":
    raise SystemExit(main())
