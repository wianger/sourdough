#!/usr/bin/env python3
"""Run Datagrump over all restored traces and compute contest scores."""

from __future__ import annotations

import argparse
import json
import math
import os
import re
import shutil
import signal
import subprocess
import sys
import time
from dataclasses import asdict, dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[1]


@dataclass(frozen=True)
class TraceConfig:
    name: str
    trace_dir: str
    loss_rate: float
    rtt_delay_ms: int
    queue_kbytes: float

    @property
    def one_way_delay_ms(self) -> int:
        return self.rtt_delay_ms // 2

    @property
    def queue_bytes(self) -> int:
        return int(round(self.queue_kbytes * 1000))


@dataclass
class LinkLogStats:
    opportunities: int = 0
    arrivals: int = 0
    arrival_bytes: int = 0
    deliveries: int = 0
    delivery_bytes: int = 0
    dropped_packets: int = 0
    dropped_bytes: int = 0
    delivery_delays_ms: list[int] | None = None

    def __post_init__(self) -> None:
        if self.delivery_delays_ms is None:
            self.delivery_delays_ms = []


@dataclass
class ScoreResult:
    trace: str
    uplink_trace: str
    downlink_trace: str
    loss_config: float
    rtt_delay_ms: int
    one_way_delay_ms: int
    queue_kbytes: float
    capacity_mbps: float
    throughput_mbps: float
    queueing_delay_ms: float
    sent_packets: int
    received_packets: int
    lost_packets: int
    queue_dropped_packets: int
    throughput_score: float
    delay_inflation: float
    delay_score: float
    loss_rate: float
    loss_score: float
    total_score: float
    output_dir: str


TRACE_CONFIGS = [
    TraceConfig("trace1", "trace1", 0.0, 60, 393.75),
    TraceConfig("trace2", "trace2", 0.0, 80, 1200.0),
    TraceConfig("trace3", "trace3", 0.0, 40, 300.0),
    TraceConfig("trace4", "trace4", 0.0, 50, 375.0),
    TraceConfig("trace5", "trace5", 0.0, 100, 4500.0),
    TraceConfig("trace6", "trace6", 0.1, 50, 31.25),
    TraceConfig("trace7", "trace7", 0.0, 20, 9.0),
    TraceConfig("trace8", "trace8", 0.0, 400, 6000.0),
    TraceConfig("trace9", "trace9", 0.0, 50, 1562.5),
    TraceConfig("trace10", "trace10", 0.0, 60, 1500.0),
]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--sender", type=Path, default=REPO_ROOT / "datagrump" / "sender"
    )
    parser.add_argument(
        "--receiver", type=Path, default=REPO_ROOT / "datagrump" / "receiver"
    )
    parser.add_argument(
        "--traces-dir", type=Path, default=REPO_ROOT / "datagrump" / "traces"
    )
    parser.add_argument("--output-dir", type=Path, default=REPO_ROOT / "results")
    parser.add_argument("--base-port", type=int, default=21000)
    parser.add_argument("--graph-bin-ms", type=int, default=500)
    parser.add_argument(
        "--trace",
        action="append",
        default=[],
        help="trace to run, e.g. trace1; repeat to run several",
    )
    parser.add_argument(
        "--skip-run",
        action="store_true",
        help="reuse existing contest logs and only recompute graphs/scores",
    )
    parser.add_argument(
        "--no-queue-limit",
        action="store_true",
        help="do not apply the table's queue_kbytes value as an uplink droptail queue",
    )
    return parser.parse_args()


def require_executable(path: Path) -> None:
    if not path.exists():
        raise SystemExit(f"missing executable: {path}")
    if not os.access(path, os.X_OK):
        raise SystemExit(f"not executable: {path}")


def require_tool(name: str) -> None:
    if shutil.which(name) is None:
        raise SystemExit(f"missing tool in PATH: {name}")


def selected_configs(names: list[str]) -> list[TraceConfig]:
    if not names:
        return TRACE_CONFIGS

    normalized = set(names)
    picked: list[TraceConfig] = []
    for config in TRACE_CONFIGS:
        aliases = {config.name, config.trace_dir}
        if normalized & aliases:
            picked.append(config)

    missing = normalized - {
        alias
        for config in picked
        for alias in (config.name, config.trace_dir)
    }
    if missing:
        raise SystemExit(f"unknown trace name(s): {', '.join(sorted(missing))}")

    return picked


def one_matching_trace_file(trace_dir: Path, suffix: str) -> Path:
    matches = sorted(path for path in trace_dir.iterdir() if path.suffix == suffix)
    if not matches:
        raise SystemExit(f"missing {suffix} trace file under {trace_dir}")
    if len(matches) > 1:
        names = ", ".join(path.name for path in matches)
        raise SystemExit(f"multiple {suffix} trace files under {trace_dir}: {names}")
    return matches[0]


def resolve_trace_paths(config: TraceConfig, traces_dir: Path) -> tuple[Path, Path]:
    trace_dir = traces_dir / config.trace_dir
    if not trace_dir.is_dir():
        raise SystemExit(f"missing trace directory: {trace_dir}")

    uplink_trace = one_matching_trace_file(trace_dir, ".down")
    downlink_trace = one_matching_trace_file(trace_dir, ".up")
    return uplink_trace, downlink_trace


def run_experiment(
    config: TraceConfig,
    args: argparse.Namespace,
    uplink_trace_path: Path,
    downlink_trace_path: Path,
    output_dir: Path,
    port: int,
) -> None:
    if output_dir.exists():
        shutil.rmtree(output_dir)
    output_dir.mkdir(parents=True)

    receiver_stdout = (output_dir / "receiver.out").open("w", encoding="utf-8")
    receiver_stderr = (output_dir / "receiver.err").open("w", encoding="utf-8")
    receiver = subprocess.Popen(
        [str(args.receiver), str(port)],
        stdout=receiver_stdout,
        stderr=receiver_stderr,
    )

    try:
        time.sleep(1)
        if receiver.poll() is not None:
            raise RuntimeError(
                f"receiver exited early with status {receiver.returncode}"
            )

        command = [
            "mm-delay",
            str(config.one_way_delay_ms),
            "mm-loss",
            "uplink",
            f"{config.loss_rate:g}",
            "mm-link",
            str(uplink_trace_path),
            str(downlink_trace_path),
            "--once",
            "--uplink-log=./contest_uplink_log",
            "--downlink-log=./contest_downlink_log",
        ]
        if not args.no_queue_limit:
            command.extend(
                [
                    "--uplink-queue=droptail",
                    f"--uplink-queue-args=bytes={config.queue_bytes}",
                ]
            )
        command.extend(
            [
                "--",
                "bash",
                "-c",
                'exec "$0" "$MAHIMAHI_BASE" "$1"',
                str(args.sender),
                str(port),
            ]
        )

        with (output_dir / "mm-command.txt").open(
            "w", encoding="utf-8"
        ) as command_file:
            command_file.write(" ".join(command) + "\n")

        with (output_dir / "mm-link.out").open("w", encoding="utf-8") as stdout:
            with (output_dir / "mm-link.err").open("w", encoding="utf-8") as stderr:
                subprocess.run(
                    command, cwd=output_dir, stdout=stdout, stderr=stderr, check=True
                )
    finally:
        stop_process(receiver)
        receiver_stdout.close()
        receiver_stderr.close()


def stop_process(process: subprocess.Popen[str]) -> None:
    if process.poll() is not None:
        return

    process.send_signal(signal.SIGTERM)
    try:
        process.wait(timeout=2)
    except subprocess.TimeoutExpired:
        process.kill()
        process.wait(timeout=2)


def run_graph(output_dir: Path, graph_bin_ms: int) -> str:
    stats_path = output_dir / "graph.stats"
    with (output_dir / "a.svg").open("w", encoding="utf-8") as stdout:
        with stats_path.open("w", encoding="utf-8") as stderr:
            subprocess.run(
                ["mm-throughput-graph", str(graph_bin_ms), "./contest_uplink_log"],
                cwd=output_dir,
                stdout=stdout,
                stderr=stderr,
                check=True,
            )
    return stats_path.read_text(encoding="utf-8")


def parse_graph_stats(stats_text: str) -> tuple[float, float, float]:
    capacity_match = re.search(r"Average capacity:\s+([0-9.]+)\s+Mbits/s", stats_text)
    throughput_match = re.search(
        r"Average throughput:\s+([0-9.]+)\s+Mbits/s", stats_text
    )
    delay_match = re.search(
        r"95th percentile per-packet queueing delay:\s+([0-9.]+)\s+ms", stats_text
    )

    if capacity_match is None or throughput_match is None or delay_match is None:
        raise ValueError(f"could not parse graph stats:\n{stats_text}")

    return (
        float(capacity_match.group(1)),
        float(throughput_match.group(1)),
        float(delay_match.group(1)),
    )


def parse_link_log(path: Path) -> LinkLogStats:
    stats = LinkLogStats()

    with path.open("r", encoding="utf-8") as log:
        for line in log:
            if not line or line.startswith("#"):
                continue
            parts = line.split()
            if len(parts) < 3:
                continue

            marker = parts[1]
            if marker == "#":
                stats.opportunities += 1
            elif marker == "+":
                stats.arrivals += 1
                stats.arrival_bytes += int(parts[2])
            elif marker == "-":
                stats.deliveries += 1
                stats.delivery_bytes += int(parts[2])
                if len(parts) >= 4:
                    stats.delivery_delays_ms.append(int(float(parts[3])))
            elif marker == "d" and len(parts) >= 4:
                stats.dropped_packets += int(parts[2])
                stats.dropped_bytes += int(parts[3])

    return stats


def percentile(values: list[int], percent: float) -> float:
    if not values:
        return 0.0
    ordered = sorted(values)
    index = max(0, min(len(ordered) - 1, math.ceil(percent / 100.0 * len(ordered)) - 1))
    return float(ordered[index])


def compute_delay_score(
    queueing_delay_ms: float, one_way_delay_ms: float
) -> tuple[float, float]:
    delay_inflation = (
        queueing_delay_ms / one_way_delay_ms if one_way_delay_ms > 0 else 0.0
    )
    if delay_inflation <= 10:
        delay_score = 20 + 80 * (10 - delay_inflation) / 10
    else:
        delay_score = 200 / delay_inflation
    return delay_inflation, delay_score


def score_one(
    config: TraceConfig,
    uplink_trace_path: Path,
    downlink_trace_path: Path,
    output_dir: Path,
    graph_bin_ms: int,
) -> ScoreResult:
    stats_text = run_graph(output_dir, graph_bin_ms)
    capacity_mbps, throughput_mbps, queueing_delay_ms = parse_graph_stats(stats_text)

    uplink = parse_link_log(output_dir / "contest_uplink_log")
    downlink = parse_link_log(output_dir / "contest_downlink_log")
    if queueing_delay_ms == 0 and uplink.delivery_delays_ms:
        queueing_delay_ms = percentile(uplink.delivery_delays_ms, 95)

    sent_packets = uplink.arrivals
    received_packets = downlink.arrivals
    lost_packets = max(0, sent_packets - received_packets)
    loss_rate = lost_packets / sent_packets if sent_packets else 0.0

    throughput_score = (
        100 * throughput_mbps / capacity_mbps if capacity_mbps > 0 else 0.0
    )
    delay_inflation, delay_score = compute_delay_score(
        queueing_delay_ms, config.one_way_delay_ms
    )
    loss_score = 100 * (1 - loss_rate)
    total_score = 0.35 * throughput_score + 0.35 * delay_score + 0.30 * loss_score

    return ScoreResult(
        trace=config.name,
        uplink_trace=str(uplink_trace_path),
        downlink_trace=str(downlink_trace_path),
        loss_config=config.loss_rate,
        rtt_delay_ms=config.rtt_delay_ms,
        one_way_delay_ms=config.one_way_delay_ms,
        queue_kbytes=config.queue_kbytes,
        capacity_mbps=capacity_mbps,
        throughput_mbps=throughput_mbps,
        queueing_delay_ms=queueing_delay_ms,
        sent_packets=sent_packets,
        received_packets=received_packets,
        lost_packets=lost_packets,
        queue_dropped_packets=uplink.dropped_packets,
        throughput_score=throughput_score,
        delay_inflation=delay_inflation,
        delay_score=delay_score,
        loss_rate=loss_rate,
        loss_score=loss_score,
        total_score=total_score,
        output_dir=str(output_dir),
    )


def write_reports(results: list[ScoreResult], output_dir: Path) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)

    with (output_dir / "scores.json").open("w", encoding="utf-8") as json_file:
        json.dump(
            {
                "total_score": total_score_sum(results),
                "results": [asdict(result) for result in results],
            },
            json_file,
            indent=2,
        )
        json_file.write("\n")


def total_score_sum(results: list[ScoreResult]) -> float:
    if not results:
        return 0.0
    return sum(result.total_score for result in results)


def format_markdown_row(result: ScoreResult) -> str:
    return (
        f"| {result.trace} "
        f"| {result.capacity_mbps:.2f} "
        f"| {result.throughput_mbps:.2f} "
        f"| {result.queueing_delay_ms:.0f} ms "
        f"| {result.loss_rate * 100:.2f}% "
        f"| {result.throughput_score:.2f} "
        f"| {result.delay_score:.2f} "
        f"| {result.loss_score:.2f} "
        f"| {result.total_score:.2f} |"
    )


def print_summary(results: list[ScoreResult]) -> None:
    print(
        "| trace | capacity | throughput | qdelay95 | loss | throughput score | delay score | loss score | total |"
    )
    print("|---|---:|---:|---:|---:|---:|---:|---:|---:|")
    for result in results:
        print(format_markdown_row(result))
    print(f"\nTotal score: {total_score_sum(results):.2f}")


def main() -> None:
    args = parse_args()
    args.sender = args.sender.resolve()
    args.receiver = args.receiver.resolve()
    args.traces_dir = args.traces_dir.resolve()
    args.output_dir = args.output_dir.resolve()

    require_executable(args.sender)
    require_executable(args.receiver)
    require_tool("mm-delay")
    require_tool("mm-loss")
    require_tool("mm-link")
    require_tool("mm-throughput-graph")

    configs = selected_configs(args.trace)
    results: list[ScoreResult] = []

    for index, config in enumerate(configs, start=1):
        uplink_trace_path, downlink_trace_path = resolve_trace_paths(
            config, args.traces_dir
        )

        output_dir = args.output_dir / config.name
        if not args.skip_run:
            print(
                f"Running {config.name}: delay={config.one_way_delay_ms}ms, "
                f"loss={config.loss_rate:g}, queue={config.queue_kbytes:g}KB, "
                f"uplink={uplink_trace_path.name}, downlink={downlink_trace_path.name}",
                flush=True,
            )
            run_experiment(
                config,
                args,
                uplink_trace_path,
                downlink_trace_path,
                output_dir,
                args.base_port + index,
            )
        else:
            print(f"Scoring existing logs for {config.name}", flush=True)

        result = score_one(
            config,
            uplink_trace_path,
            downlink_trace_path,
            output_dir,
            args.graph_bin_ms,
        )
        results.append(result)
        print(format_markdown_row(result), flush=True)

    write_reports(results, args.output_dir)
    print()
    print_summary(results)
    print(f"\nWrote reports under {args.output_dir}")


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        sys.exit(130)
