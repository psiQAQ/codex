"""Isolated black-box runs; offline checks never invoke Codex or a model."""

import hashlib
import json
import os
import re
import signal
import subprocess
import sys
import time
from contextlib import nullcontext
from datetime import datetime, timezone
from pathlib import Path

from .fixtures import CASES, check_answer, materialize
from .report import summarize, write_reports
from .usage import aggregate, read_events


def execute(command, cwd, environment, timeout):
    """Terminate owned descendants and bound final pipe collection at deadline."""
    if os.name == "nt":
        from .windows_job import WindowsJob

        job_context = WindowsJob()
        options = {
            "creationflags": subprocess.CREATE_NEW_PROCESS_GROUP | 0x4
        }  # CREATE_SUSPENDED
    else:
        job_context = nullcontext()
        options = {"start_new_session": True}
    with job_context as job:
        process = subprocess.Popen(
            command,
            cwd=cwd,
            env=environment,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            **options,
        )
        try:
            if job:
                job.attach_and_resume(process.pid)
            status = "completed"
            try:
                stdout, stderr = process.communicate(timeout=timeout)
            except (subprocess.TimeoutExpired, KeyboardInterrupt) as error:
                status = (
                    "timeout"
                    if isinstance(error, subprocess.TimeoutExpired)
                    else "cancelled"
                )
                if job:
                    job.terminate()
                else:
                    try:
                        os.killpg(process.pid, signal.SIGKILL)
                    except ProcessLookupError:
                        pass
                try:
                    stdout, stderr = process.communicate(timeout=1)
                except subprocess.TimeoutExpired as cleanup:
                    stdout, stderr = cleanup.stdout or b"", cleanup.stderr or b""
                    status += "_cleanup_incomplete"
            if status == "completed" and process.returncode != 0:
                status = "failed"
            return stdout, stderr, process.poll(), status
        except BaseException:
            # Setup failures occur while the Windows root is still suspended.
            # A bounded root wait avoids replacing the original failure.
            if process.poll() is None:
                process.kill()
                try:
                    process.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    pass
            raise


AUTH_OVERRIDE_ENV_VARS = frozenset(
    {
        "OPENAI_API_KEY",
        "CODEX_API_KEY",
        "CODEX_ACCESS_TOKEN",
        "OPENAI_FEDERATION_RULE_ID",
        "OPENAI_IDENTITY_TOKEN_FILE",
        "OPENAI_WORKLOAD_IDENTITY_CONTEXT",
    }
)


def isolated_environment(home):
    # Windows environment names are case-insensitive. Remove every spelling
    # from this copy, including competing CODEX_HOME spellings, before launch.
    environment = {
        name: value
        for name, value in os.environ.items()
        if name.upper() not in AUTH_OVERRIDE_ENV_VARS | {"CODEX_HOME"}
    }
    environment["CODEX_HOME"] = str(Path(home).resolve())
    return environment


def scoped_usage(paths, stdout):
    """Use the actual exec root thread and persisted root turn IDs."""
    roots = set()
    root_thread = None
    for line in stdout.decode("utf-8").splitlines():
        event = json.loads(line)
        if event.get("type") == "thread.started":
            root_thread = event.get("thread_id")
            break
    if not root_thread:
        result = aggregate([])
        result["errors"] = [
            "exec did not identify its root thread; usage scope is unknown"
        ]
        return result, {"root_thread": None, "root_turn_ids": []}
    threads = {}
    for path, _, event in read_events(paths):
        payload = event.get("payload", {})
        if event.get("type") == "session_meta" and isinstance(payload, dict):
            threads[path] = payload.get("id")
        if (
            event.get("type") == "token_usage_record"
            and isinstance(payload, dict)
            and payload.get("thread_id") == root_thread
        ):
            if isinstance(payload.get("root_turn_id"), str):
                roots.add(payload["root_turn_id"])
        if (
            threads.get(path) == root_thread
            and event.get("type") == "event_msg"
            and isinstance(payload, dict)
            and payload.get("type") in {"task_started", "turn_started"}
        ):
            if isinstance(payload.get("turn_id"), str):
                roots.add(payload["turn_id"])
    result = aggregate(paths, roots) if roots else aggregate([])
    return result, {"root_thread": root_thread, "root_turn_ids": sorted(roots)}


def verify_configuration(paths, scope, manifest):
    """Validate effective root and child turn settings in authoritative rollouts."""
    contexts = []
    threads = {}
    for path, _, event in read_events(paths):
        payload = event.get("payload", {})
        if event.get("type") == "session_meta" and isinstance(payload, dict):
            threads[path] = payload.get("id")
        if event.get("type") != "turn_context" or not isinstance(payload, dict):
            continue
        thread = threads.get(path)
        if (
            thread != scope["root_thread"]
            and payload.get("root_turn_id") not in scope["root_turn_ids"]
        ):
            continue
        if (
            thread == scope["root_thread"]
            and payload.get("turn_id") not in scope["root_turn_ids"]
        ):
            continue
        contexts.append(
            {
                "thread_id": thread,
                "turn_id": payload.get("turn_id"),
                "model": payload.get("model"),
                "effort": payload.get("effort"),
            }
        )
    selected_turns = set()
    for _, _, event in read_events(paths):
        payload = event.get("payload", {})
        if (
            event.get("type") == "token_usage_record"
            and isinstance(payload, dict)
            and payload.get("root_turn_id") in scope["root_turn_ids"]
        ):
            selected_turns.add((payload.get("thread_id"), payload.get("turn_id")))
    covered_turns = {(context["thread_id"], context["turn_id"]) for context in contexts}
    root_contexts = [
        context for context in contexts if context["thread_id"] == scope["root_thread"]
    ]
    if (
        not root_contexts
        or not selected_turns.issubset(covered_turns)
        or any(not context["model"] or not context["effort"] for context in contexts)
    ):
        return {
            "status": "missing",
            "source": "rollout.turn_context",
            "contexts": contexts,
        }
    if any(
        context["model"] != manifest["model"] or context["effort"] != manifest["effort"]
        for context in contexts
    ):
        return {
            "status": "mismatch",
            "source": "rollout.turn_context",
            "contexts": contexts,
        }
    return {
        "status": "verified",
        "source": "rollout.turn_context",
        "contexts": contexts,
    }


def schedule(tasks, conditions, repeats):
    for repeat in range(repeats):
        for task in tasks:
            order = conditions if repeat % 2 == 0 else list(reversed(conditions))
            for condition in order:
                yield task, condition, repeat


def _slug(value):
    if (
        not isinstance(value, str)
        or not value
        or any(
            character
            not in "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789_-"
            for character in value
        )
    ):
        raise ValueError(
            "task and condition names must contain only letters, digits, _ or -"
        )
    return value


def load_manifest(path):
    manifest = json.loads(Path(path).read_text(encoding="utf-8"))
    if manifest.get("schema_version") != 1 or manifest.get("effort") != "high":
        raise ValueError("manifest requires schema_version 1 and high effort")
    if type(manifest.get("seed")) is not int:
        raise ValueError("manifest requires an integer seed")
    if not isinstance(manifest.get("model"), str) or not manifest["model"]:
        raise ValueError("manifest requires an explicit model")
    tasks = manifest.get("tasks")
    if not isinstance(tasks, list) or not tasks or len(set(tasks)) != len(tasks):
        raise ValueError("manifest requires unique tasks")
    for task in tasks:
        _slug(task)
        if task not in CASES:
            raise ValueError(f"unknown fixture {task}")
    return manifest


def _report(attempts, manifest, mode):
    return {
        "schema_version": 1,
        "mode": mode,
        "model": manifest["model"],
        "effort": manifest["effort"],
        "seed": manifest["seed"],
        "manifest_sha256": hashlib.sha256(
            json.dumps(manifest, sort_keys=True).encode()
        ).hexdigest(),
        "summary": summarize(attempts),
        "attempts": attempts,
    }


def self_check(manifest, run_dir):
    """Exercise real local process I/O and the oracle, without fake model usage."""
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=False)
    attempts = []
    for task in manifest["tasks"]:
        workspace = run_dir / task
        expected = materialize(workspace, task, manifest["seed"])
        started = time.perf_counter()
        command = [
            sys.executable,
            "-c",
            "import pathlib,sys; p=pathlib.Path('.'); sys.stdout.buffer.write((p/'stdout.bin').read_bytes()); sys.stderr.buffer.write((p/'stderr.bin').read_bytes()); sys.exit(int(sys.argv[1]))",
            str(expected["exit_code"]),
        ]
        result = subprocess.run(
            command, cwd=workspace, capture_output=True, check=False
        )
        observed = {
            "stdout_sha256": hashlib.sha256(result.stdout).hexdigest(),
            "stderr_sha256": hashlib.sha256(result.stderr).hexdigest(),
            "exit_code": result.returncode,
        }
        io_passed = all(observed[key] == expected[key] for key in observed)
        (workspace / "answer.json").write_text(
            json.dumps(expected["answer"]), encoding="utf-8"
        )
        oracle = check_answer(workspace, expected)
        attempts.append(
            {
                "task": task,
                "condition": "offline_fixture",
                "repeat": 0,
                "status": "passed" if io_passed and oracle["passed"] else "failed",
                "oracle": oracle,
                "wall_seconds": time.perf_counter() - started,
                "raw_stdout_bytes": len(result.stdout),
                "raw_stderr_bytes": len(result.stderr),
                "usage": aggregate([]),
            }
        )
    report = _report(attempts, manifest, "offline_self_check")
    write_reports(run_dir, report)
    return report


def run(manifest, conditions, repeats, run_dir, timeout):
    """Run explicitly supplied binaries and already-authenticated isolated homes.

    A supplied home must not be the user's default/global Codex home. It is
    never populated with copied authentication, and no model fallback occurs.
    """
    if type(repeats) is not int or repeats < 1 or timeout <= 0:
        raise ValueError("repeats and timeout must be positive")
    default_home = Path.home() / ".codex"
    global_home = Path(os.environ.get("CODEX_HOME", default_home)).resolve()
    if not isinstance(conditions, list) or not conditions:
        raise ValueError("provide a nonempty conditions array")
    if not all(isinstance(condition, dict) for condition in conditions):
        raise ValueError("each condition must be an object")
    if len({condition["name"] for condition in conditions}) != len(conditions):
        raise ValueError("provide unique condition names")
    if len({str(Path(condition["home"]).resolve()) for condition in conditions}) != len(
        conditions
    ):
        raise ValueError("each condition needs a separate isolated home")
    condition_metadata = {}
    configs = {}
    for condition in conditions:
        _slug(condition["name"])
        binary = Path(condition["executable"]).resolve()
        home = Path(condition["home"]).resolve()
        if not binary.is_file():
            raise ValueError(f"missing executable for {condition['name']}")
        if home in {default_home.resolve(), global_home}:
            raise ValueError("run requires a dedicated isolated home")
        if not (home / "auth.json").is_file():
            raise ValueError(f"isolated home for {condition['name']} has no auth.json")
        if list((home / "sessions").rglob("*.jsonl")):
            raise ValueError(
                "isolated home already contains rollouts; use an unused dedicated home"
            )
        config = {}
        if "config" in condition:
            config = json.loads(Path(condition["config"]).read_text(encoding="utf-8"))
            if not isinstance(config, dict) or any(
                not isinstance(key, str)
                or re.fullmatch(r"[A-Za-z_][A-Za-z0-9_.]*", key) is None
                or any(
                    "model" in part
                    or "profile" in part
                    or part in {"reasoning_effort", "collaboration_mode", "projects"}
                    for part in key.split(".")
                )
                for key in config
            ):
                raise ValueError(
                    "config overrides require valid keys and cannot change model, effort or provider"
                )
            if any(type(value) not in {str, int, bool} for value in config.values()):
                raise ValueError(
                    "config override values must be strings, integers or booleans"
                )
        configs[condition["name"]] = config
        with binary.open("rb") as stream:
            binary_hash = hashlib.file_digest(stream, "sha256").hexdigest()
        environment = isolated_environment(home)
        version_stdout, _, version_code, version_status = execute(
            [str(binary), "--version"], home, environment, 10
        )
        if (
            version_status != "completed"
            or version_code != 0
            or not version_stdout.strip()
        ):
            raise ValueError(f"cannot obtain client version for {condition['name']}")
        condition_metadata[condition["name"]] = {
            "client_sha256": binary_hash,
            "client_version": version_stdout.decode("utf-8", errors="replace").strip(),
            "config_sha256": hashlib.sha256(
                json.dumps(config, sort_keys=True).encode()
            ).hexdigest(),
            "cache_stratum": condition.get("cache_stratum", "unspecified"),
        }
    run_dir = Path(run_dir)
    run_dir.mkdir(parents=True, exist_ok=False)
    attempts = []
    seen_paths = set()
    for task, condition, repeat in schedule(manifest["tasks"], conditions, repeats):
        workspace = run_dir / f"{task}-{condition['name']}-{repeat}"
        expected = materialize(workspace, task, manifest["seed"])
        prompt = f"Read stdout.bin and stderr.bin as synthetic command output, and command-result.json for exit code. Write answer.json with exactly case, status, evidence. case is {task}. Identify the EVIDENCE marker(s) from output; preserve their spelling. status is failed for nonzero exit code, otherwise ok, unless output reports an explicit lifecycle status, which you must preserve. If there are multiple errors, report their evidence markers as an ordered JSON list. Do not change input files."
        command = [
            str(Path(condition["executable"]).resolve()),
            "exec",
            "--json",
            "--ignore-user-config",
            "--strict-config",
            "--sandbox",
            "workspace-write",
            "--skip-git-repo-check",
            "--model",
            manifest["model"],
            "-c",
            'model_reasoning_effort="high"',
            "--cd",
            str(workspace.resolve()),
        ]
        # Codex accepts config overrides, not arbitrary alternative config files.
        # Parse the file as an explicit flat override map, and pass JSON values.
        for key, value in configs[condition["name"]].items():
            command += ["-c", f"{key}={json.dumps(value)}"]
        command += [prompt]
        environment = isolated_environment(condition["home"])
        started = time.perf_counter()
        server_time = datetime.now(timezone.utc).isoformat()
        status = "failed"
        try:
            stdout, stderr, returncode, status = execute(
                command, workspace, environment, timeout
            )
        except OSError as error:
            stdout, stderr, returncode, status = (
                b"",
                str(error).encode(),
                None,
                "failed",
            )
        elapsed = time.perf_counter() - started
        (workspace / "exec.jsonl").write_bytes(stdout)
        (workspace / "exec.stderr").write_bytes(stderr)
        paths = sorted((Path(condition["home"]) / "sessions").rglob("*.jsonl"))
        new_paths = [path for path in paths if str(path) not in seen_paths]
        seen_paths.update(str(path) for path in paths)
        scope = {"root_thread": None, "root_turn_ids": []}
        configuration = {
            "status": "missing",
            "source": "rollout.turn_context",
            "contexts": [],
        }
        try:
            usage, scope = scoped_usage(new_paths, stdout)
            configuration = verify_configuration(new_paths, scope, manifest)
        except (ValueError, UnicodeDecodeError) as error:
            usage = {
                "status": "invalid",
                "observed_usage": None,
                "complete_total": None,
                "errors": [str(error)],
            }
        oracle = check_answer(workspace, expected)
        if status == "completed":
            status = "passed" if oracle["passed"] else "failed"
            if configuration["status"] != "verified":
                status = "configuration_unverified"
        if b"model rerouted:" in stdout:
            configuration["status"] = "rerouted"
            if status == "passed":
                status = "configuration_unverified"
        attempts.append(
            {
                "task": task,
                "condition": condition["name"],
                "repeat": repeat,
                "status": status,
                "returncode": returncode,
                "oracle": oracle,
                "usage": usage,
                "scope": scope,
                "configuration": configuration,
                "wall_seconds": elapsed,
                "service_started_utc": server_time,
                "raw_stdout_bytes": len(stdout),
                "raw_stderr_bytes": len(stderr),
                "fixture_stdout_bytes": expected["stdout_bytes"],
                "fixture_stderr_bytes": expected["stderr_bytes"],
                "rollout_files": len(new_paths),
                "cache_stratum": condition.get("cache_stratum", "unspecified"),
            }
        )
        report = _report(attempts, manifest, "explicit_model_run")
        report["conditions"] = condition_metadata
        write_reports(run_dir, report)
        if status == "cancelled":
            break
    return report
