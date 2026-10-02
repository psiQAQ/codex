"""Synthetic output corpus and deterministic structured-answer oracles."""

import hashlib
import json
import random
from pathlib import Path

CASES = (
    "middle_error",
    "multiple_failures",
    "empty_stdout",
    "stderr_only",
    "long_json_line",
    "unicode_crlf",
    "cancelled",
    "quota_reached",
    "expired_reference",
    "repeated_query",
    "missing_metadata",
    "cross_session_reference",
)


def materialize(workspace, case, seed=20261002):
    if case not in CASES:
        raise ValueError(f"unknown fixture: {case}")
    workspace = Path(workspace)
    workspace.mkdir(parents=True, exist_ok=False)
    rng = random.Random(f"{seed}:{case}")
    marker = f"EVIDENCE-{rng.randrange(1_000_000):06d}"
    stdout, stderr, exit_code = b"", b"", 0
    answer = {"case": case, "status": "ok", "evidence": marker}
    if case == "middle_error":
        stdout = ("prefix\n" * 3000 + f"ERROR {marker}\n" + "suffix\n" * 3000).encode()
        exit_code = 1
        answer["status"] = "failed"
    elif case == "multiple_failures":
        stdout = f"ERROR {marker}-A\nERROR {marker}-B\n".encode()
        exit_code = 2
        answer.update(status="failed", evidence=[f"{marker}-A", f"{marker}-B"])
    elif case in ("empty_stdout", "stderr_only"):
        stderr = f"diagnostic {marker}\n".encode()
        if case == "stderr_only":
            exit_code = 3
            answer["status"] = "failed"
    elif case == "long_json_line":
        stdout = (
            json.dumps({"padding": "x" * 60000, "evidence": marker}).encode() + b"\n"
        )
    elif case == "unicode_crlf":
        stdout = f"量子化学 αβ 🧪\r\n{marker}\r\n".encode()
    else:
        statuses = {
            "cancelled": "cancelled",
            "quota_reached": "quota_reached",
            "expired_reference": "expired",
            "repeated_query": "ok",
            "missing_metadata": "metadata_missing",
            "cross_session_reference": "denied",
        }
        answer["status"] = statuses[case]
        stdout = (json.dumps(answer, ensure_ascii=False) + "\n").encode()
    (workspace / "stdout.bin").write_bytes(stdout)
    (workspace / "stderr.bin").write_bytes(stderr)
    expectation = {
        "answer": answer,
        "exit_code": exit_code,
        "stdout_bytes": len(stdout),
        "stderr_bytes": len(stderr),
        "stdout_sha256": hashlib.sha256(stdout).hexdigest(),
        "stderr_sha256": hashlib.sha256(stderr).hexdigest(),
    }
    (workspace / "command-result.json").write_text(
        json.dumps({"exit_code": exit_code}) + "\n", encoding="utf-8"
    )
    return expectation


def check_answer(workspace, expected):
    path = Path(workspace) / "answer.json"
    try:
        actual = json.loads(path.read_text(encoding="utf-8"))
    except UnicodeDecodeError as error:
        return {"passed": False, "reason": f"answer.json is not valid UTF-8: {error}"}
    except (OSError, json.JSONDecodeError) as error:
        return {"passed": False, "reason": str(error)}
    return {
        "passed": actual == expected["answer"],
        "reason": "matched structured answer"
        if actual == expected["answer"]
        else "structured answer differs",
    }
