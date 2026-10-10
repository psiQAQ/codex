"""Observed Codex usage, with explicit provenance and incomplete totals."""

import json
from pathlib import Path

FIELDS = (
    "input_tokens",
    "cached_input_tokens",
    "output_tokens",
    "reasoning_output_tokens",
)


def _usage(value):
    if not isinstance(value, dict):
        raise ValueError("missing usage object")
    result = {}
    for field in FIELDS:
        number = value.get(field)
        if type(number) is not int or number < 0:
            raise ValueError(f"missing or invalid {field}")
        result[field] = number
    if result["cached_input_tokens"] > result["input_tokens"]:
        raise ValueError("cached input exceeds input")
    if result["reasoning_output_tokens"] > result["output_tokens"]:
        raise ValueError("reasoning output exceeds output")
    if "total_tokens" in value and (
        type(value["total_tokens"]) is not int
        or value["total_tokens"] != result["input_tokens"] + result["output_tokens"]
    ):
        raise ValueError("total tokens disagree with input plus output")
    return result


def read_events(paths):
    """Keep source locations; malformed JSON is a measurement error."""
    for path in paths:
        with Path(path).open(encoding="utf-8") as stream:
            for line_number, line in enumerate(stream, 1):
                if line.strip():
                    try:
                        event = json.loads(line)
                    except json.JSONDecodeError as error:
                        raise ValueError(
                            f"{path}:{line_number}: {error.msg}"
                        ) from error
                    if not isinstance(event, dict):
                        raise ValueError(
                            f"{path}:{line_number}: event must be an object"
                        )
                    yield str(path), line_number, event


def aggregate(paths, root_turn_ids=None):
    """Prefer independent response records; never combine sources per thread.

    Counters are snapshots, not response usage. Counter-only streams cannot
    select a causal root turn. Their last snapshot includes any earlier history.
    """
    records = {}
    snapshots = {}
    origins = {}
    problems = []
    duplicates = 0
    contexts = {}
    record_threads = set()
    for path, line, event in read_events(paths):
        location = f"{path}:{line}"
        kind = event.get("type")
        payload = event.get("payload", {})
        if kind == "session_meta":
            if not isinstance(payload, dict) or not isinstance(payload.get("id"), str):
                problems.append(f"{location}: missing session identity")
                continue
            contexts[path] = payload.get("id")
        elif kind == "thread.started":
            contexts[path] = event.get("thread_id")
        elif kind == "token_usage_record":
            if not isinstance(payload, dict):
                problems.append(f"{location}: invalid record payload")
                continue
            thread = payload.get("thread_id")
            response = payload.get("response_id")
            if isinstance(thread, str) and thread:
                record_threads.add(thread)
            if not all(
                isinstance(payload.get(key), str) and payload[key]
                for key in ("thread_id", "response_id", "turn_id", "root_turn_id")
            ):
                problems.append(f"{location}: missing response identity")
                continue
            try:
                usage = _usage(payload.get("usage"))
                cumulative = _usage(payload.get("thread_token_usage"))
                turn_usage = _usage(payload.get("turn_token_usage"))
            except ValueError as error:
                problems.append(f"{location}: {error}")
                continue
            key = (thread, response)
            record = {
                "thread_id": thread,
                "response_id": response,
                "turn_id": payload["turn_id"],
                "root_turn_id": payload["root_turn_id"],
                "usage": usage,
                "thread_total": cumulative,
                "turn_total": turn_usage,
            }
            if key in records:
                if records[key] != record:
                    problems.append(f"{location}: conflicting response {response}")
                else:
                    duplicates += 1
            else:
                records[key] = record
        elif (
            kind == "event_msg"
            and isinstance(payload, dict)
            and payload.get("type") == "token_count"
        ):
            info = payload.get("info")
            if info is None:
                continue
            thread = contexts.get(path)
            if not isinstance(info, dict) or not isinstance(thread, str) or not thread:
                problems.append(
                    f"{location}: counter requires session identity and info object"
                )
                continue
            origins.setdefault(thread, {}).setdefault(path, []).append(
                (location, info.get("total_token_usage"))
            )
        elif kind == "turn.completed":
            thread = contexts.get(path)
            if not isinstance(thread, str) or not thread:
                problems.append(f"{location}: exec counter requires thread identity")
                continue
            origins.setdefault(thread, {}).setdefault(path, []).append(
                (location, event.get("usage"))
            )

    # Validate authoritative cumulative snapshots against independent records.
    # A constant prefix is allowed when the supplied rollout starts after resume.
    thread_sums, turn_sums, thread_prefixes, turn_prefixes = {}, {}, {}, {}
    for record in sorted(
        records.values(),
        key=lambda item: (
            item["thread_id"],
            item["thread_total"]["input_tokens"]
            + item["thread_total"]["output_tokens"],
        ),
    ):
        thread = record["thread_id"]
        turn = (thread, record["turn_id"])
        for sums, prefixes, identity, snapshot in (
            (thread_sums, thread_prefixes, thread, record["thread_total"]),
            (turn_sums, turn_prefixes, turn, record["turn_total"]),
        ):
            sums.setdefault(identity, dict.fromkeys(FIELDS, 0))
            for field in FIELDS:
                sums[identity][field] += record["usage"][field]
            prefix = {
                field: snapshot[field] - sums[identity][field] for field in FIELDS
            }
            if any(value < 0 for value in prefix.values()) or (
                identity in prefixes and prefixes[identity] != prefix
            ):
                problems.append(f"inconsistent cumulative snapshot for {identity}")
            prefixes[identity] = prefix

    selected = [
        record
        for record in records.values()
        if root_turn_ids is None or record["root_turn_id"] in root_turn_ids
    ]
    totals = dict.fromkeys(FIELDS, 0)
    sources = set()
    for record in selected:
        for field in FIELDS:
            totals[field] += record["usage"][field]
        sources.add("token_usage_record")
    for thread, events in origins.items():
        if thread in record_threads:
            continue
        if root_turn_ids is not None:
            problems.append(f"counter-only thread {thread} cannot select root turns")
            continue
        endpoints = []
        valid = True
        for stream in events.values():
            previous = dict.fromkeys(FIELDS, 0)
            for location, value in stream:
                try:
                    current = _usage(value)
                except ValueError as error:
                    problems.append(f"{location}: {error}")
                    valid = False
                    continue
                if any(current[field] < previous[field] for field in FIELDS):
                    problems.append(f"{location}: cumulative counter decreased")
                    valid = False
                previous = current
            endpoints.append(previous)
        # Files can overlap, be replayed, or arrive in either order. Preserve
        # monotonicity within each file and require comparable final snapshots.
        latest = max(
            endpoints, key=lambda item: item["input_tokens"] + item["output_tokens"]
        )
        if any(
            any(endpoint[field] > latest[field] for field in FIELDS)
            for endpoint in endpoints
        ):
            problems.append(f"incomparable cumulative snapshots for {thread}")
            valid = False
        if valid:
            snapshots[thread] = latest
            for field in FIELDS:
                totals[field] += latest[field]
            sources.add("cumulative_snapshot")
    observed = bool(selected or snapshots)
    return {
        "status": "invalid" if problems else "observed" if observed else "missing",
        "observed_usage": totals if observed else None,
        "complete_total": None,
        "sources": sorted(sources),
        "response_count": len(selected),
        "duplicate_records": duplicates,
        "threads": sorted(
            {record["thread_id"] for record in selected} | set(snapshots)
        ),
        "errors": problems,
        "limitations": [
            "Only provider/client-reported usage is counted. Unreported failed requests remain unknown.",
            "Counter-only totals include earlier thread history and cannot identify independent responses.",
        ]
        if snapshots
        else [
            "Only provider-reported completions are counted. Unreported requests remain unknown."
        ],
    }
