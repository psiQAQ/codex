"""Aggregate every attempt while preserving missing measurements."""

import json
import math
import statistics
from pathlib import Path


def summarize(attempts):
    groups = {}
    for attempt in attempts:
        groups.setdefault(attempt["condition"], []).append(attempt)
    result = {}
    for name, rows in groups.items():
        known = [row for row in rows if row["usage"]["status"] == "observed"]
        success = sum(row["status"] == "passed" for row in rows)
        observed_input = sum(
            row["usage"]["observed_usage"]["input_tokens"] for row in known
        )
        latencies = sorted(row["wall_seconds"] for row in rows)
        result[name] = {
            "attempts": len(rows),
            "successes": success,
            "success_rate": success / len(rows),
            "usage_observed_attempts": len(known),
            "observed_input_tokens": observed_input if known else None,
            "observed_input_mean": observed_input / len(known) if known else None,
            "input_per_success": None,
            "wall_seconds_median": statistics.median(latencies),
            "wall_seconds_p95": latencies[
                min(len(latencies) - 1, max(0, math.ceil(0.95 * len(latencies)) - 1))
            ],
            "limitation": "Usage covers observed responses, including failed attempts; complete billable task totals are unavailable.",
        }
    pairs = {}
    for row in attempts:
        pairs.setdefault((row["task"], row["repeat"]), []).append(row)
    differences = []
    for (task, repeat), rows in pairs.items():
        if len(rows) == 2 and all(row["usage"]["status"] == "observed" for row in rows):
            a, b = sorted(rows, key=lambda row: row["condition"])
            differences.append(
                {
                    "task": task,
                    "repeat": repeat,
                    "a": a["condition"],
                    "b": b["condition"],
                    "observed_input_b_minus_a": b["usage"]["observed_usage"][
                        "input_tokens"
                    ]
                    - a["usage"]["observed_usage"]["input_tokens"],
                    "both_passed": a["status"] == b["status"] == "passed",
                }
            )
    return {"conditions": result, "paired_differences": differences}


def write_reports(directory, report):
    directory = Path(directory)
    directory.mkdir(parents=True, exist_ok=True)
    (directory / "report.json").write_text(
        json.dumps(report, indent=2, ensure_ascii=False) + "\n", encoding="utf-8"
    )
    lines = [
        "# Context input evaluation",
        "",
        "Observed usage includes failed attempts. Cached input is part of input; reasoning output is part of output.",
        "",
        "Complete billable totals and subscription cost remain unknown. Byte counts are diagnostics, not token estimates.",
        "",
        "| Condition | Attempts | Passed | Observed usage | Observed input | Median wall seconds |",
        "|---|---:|---:|---:|---:|---:|",
    ]
    for name, item in report["summary"]["conditions"].items():
        lines.append(
            f"| {name} | {item['attempts']} | {item['successes']} | {item['usage_observed_attempts']} | {item['observed_input_tokens']} | {item['wall_seconds_median']:.3f} |"
        )
    lines += [
        "",
        "## Measurement limits",
        "",
        "- Failed requests without reported usage remain unknown.",
        "- Service-side cold cache cannot be guaranteed by a fresh local workspace.",
        "- Synthetic offline checks do not establish candidate artifact behavior or model task success.",
        "- Reports contain synthetic task identifiers and aggregate measurements; raw prompts and output stay in the private run directory.",
        "",
    ]
    (directory / "report.md").write_text("\n".join(lines), encoding="utf-8")
