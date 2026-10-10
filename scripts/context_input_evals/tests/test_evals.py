import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from scripts.context_input_evals.fixtures import CASES, check_answer, materialize
from scripts.context_input_evals.report import summarize, write_reports
from scripts.context_input_evals.runner import (
    execute,
    isolated_environment,
    load_manifest,
    run,
    schedule,
    scoped_usage,
    self_check,
    verify_configuration,
)
from scripts.context_input_evals.usage import aggregate


def tokens(number, cached=0, output=4, reasoning=2):
    return {
        "input_tokens": number,
        "cached_input_tokens": cached,
        "output_tokens": output,
        "reasoning_output_tokens": reasoning,
        "total_tokens": number + output,
    }


def record(
    response, number, thread="root", root="turn-root", cumulative=None, turn="turn-root"
):
    usage = tokens(number)
    return {
        "type": "token_usage_record",
        "payload": {
            "thread_id": thread,
            "session_id": thread,
            "turn_id": turn,
            "root_turn_id": root,
            "response_id": response,
            "usage": usage,
            "turn_token_usage": cumulative or usage,
            "thread_token_usage": cumulative or usage,
        },
    }


class WorkspaceTest(unittest.TestCase):
    def setUp(self):
        # Explicit project-local temporary directory, never system TEMP.
        base = Path(".context-dev/e0-tests")
        base.mkdir(parents=True, exist_ok=True)
        self.temporary = tempfile.TemporaryDirectory(dir=base)
        self.directory = Path(self.temporary.name)

    def tearDown(self):
        self.temporary.cleanup()

    def jsonl(self, events, name="rollout.jsonl"):
        path = self.directory / name
        path.write_text(
            "".join(json.dumps(event) + "\n" for event in events), encoding="utf-8"
        )
        return path


class UsageTests(WorkspaceTest):
    def test_independent_records_ignore_cumulative_mirrors(self):
        a, b = (
            record("response-a", 10),
            record("response-b", 20, cumulative=tokens(30, output=8, reasoning=4)),
        )
        snapshot = {
            "type": "event_msg",
            "payload": {
                "type": "token_count",
                "info": {"total_token_usage": tokens(999)},
            },
        }
        result = aggregate(
            [
                self.jsonl(
                    [
                        {"type": "session_meta", "payload": {"id": "root"}},
                        a,
                        b,
                        snapshot,
                    ]
                )
            ]
        )
        self.assertEqual(result["status"], "observed")
        self.assertEqual(result["observed_usage"]["input_tokens"], 30)
        self.assertEqual(result["response_count"], 2)
        self.assertIsNone(result["complete_total"])

    def test_duplicate_files_and_events_are_not_billed_twice(self):
        event = record("same", 10)
        result = aggregate(
            [self.jsonl([event, event]), self.jsonl([event], "copy.jsonl")]
        )
        self.assertEqual(result["duplicate_records"], 2)
        self.assertEqual(result["observed_usage"]["input_tokens"], 10)

    def test_conflicting_response_is_invalid(self):
        result = aggregate([self.jsonl([record("same", 10), record("same", 11)])])
        self.assertEqual(result["status"], "invalid")
        self.assertIn("conflicting response", result["errors"][0])

    def test_retry_and_child_usage_include_each_independent_response(self):
        events = [
            record("retry-1", 10),
            record("retry-2", 20, cumulative=tokens(30, output=8, reasoning=4)),
            record("child-1", 7, thread="child"),
            record("unrelated", 90, thread="other", root="other-turn"),
        ]
        result = aggregate([self.jsonl(events)], {"turn-root"})
        self.assertEqual(result["observed_usage"]["input_tokens"], 37)
        self.assertEqual(result["threads"], ["child", "root"])

    def test_counters_are_monotonic_snapshots_not_increments(self):
        events = [{"type": "session_meta", "payload": {"id": "root"}}] + [
            {
                "type": "event_msg",
                "payload": {
                    "type": "token_count",
                    "info": {"total_token_usage": tokens(number)},
                },
            }
            for number in (100, 250, 250)
        ]
        result = aggregate([self.jsonl(events)])
        self.assertEqual(result["observed_usage"]["input_tokens"], 250)
        self.assertEqual(result["response_count"], 0)

    def test_duplicate_counter_files_do_not_replay_cost_or_depend_on_order(self):
        metadata = {"type": "session_meta", "payload": {"id": "root"}}
        counters = [
            {
                "type": "event_msg",
                "payload": {
                    "type": "token_count",
                    "info": {"total_token_usage": tokens(number)},
                },
            }
            for number in (100, 250)
        ]
        full = self.jsonl([metadata] + counters)
        duplicate = self.jsonl([metadata] + counters, "duplicate.jsonl")
        partial = self.jsonl([metadata, counters[0]], "partial.jsonl")
        result = aggregate([full, duplicate, partial])
        self.assertEqual(result["status"], "observed")
        self.assertEqual(result["observed_usage"]["input_tokens"], 250)
        self.assertEqual(result, aggregate([partial, duplicate, full]))

    def test_malformed_identity_and_total_are_invalid_instead_of_crashing(self):
        for key, value in (("thread_id", {}), ("thread_id", [])):
            event = record("broken", 10)
            event["payload"][key] = value
            self.assertEqual(aggregate([self.jsonl([event])])["status"], "invalid")
        event = record("broken", 10)
        event["payload"]["usage"]["total_tokens"] = True
        self.assertEqual(aggregate([self.jsonl([event])])["status"], "invalid")

    def test_counter_decrease_is_invalid_without_reset_guess(self):
        events = [
            {"type": "thread.started", "thread_id": "root"},
            {"type": "turn.completed", "usage": tokens(100)},
            {"type": "turn.completed", "usage": tokens(50)},
        ]
        result = aggregate([self.jsonl(events)])
        self.assertEqual(result["status"], "invalid")
        self.assertIsNone(result["observed_usage"])

    def test_missing_usage_and_missing_fields_are_not_zero(self):
        self.assertEqual(aggregate([])["status"], "missing")
        self.assertIsNone(aggregate([])["observed_usage"])
        for field in (
            "input_tokens",
            "cached_input_tokens",
            "output_tokens",
            "reasoning_output_tokens",
        ):
            with self.subTest(field=field):
                event = record("broken", 10)
                del event["payload"]["usage"][field]
                result = aggregate([self.jsonl([event])])
                self.assertEqual(result["status"], "invalid")
                self.assertIsNone(result["observed_usage"])

    def test_negative_and_subset_errors_are_invalid(self):
        for field, value in (
            ("input_tokens", -1),
            ("cached_input_tokens", 11),
            ("reasoning_output_tokens", 5),
            ("input_tokens", True),
        ):
            with self.subTest(field=field, value=value):
                event = record("broken", 10)
                event["payload"]["usage"][field] = value
                self.assertEqual(aggregate([self.jsonl([event])])["status"], "invalid")

    def test_cumulative_record_snapshots_are_validated(self):
        events = [
            record("a", 10),
            record("b", 20, cumulative=tokens(31, output=8, reasoning=4)),
        ]
        self.assertEqual(aggregate([self.jsonl(events)])["status"], "invalid")

    def test_constant_resume_prefix_is_allowed_but_not_billed(self):
        events = [
            record("a", 10, cumulative=tokens(110, output=8, reasoning=4)),
            record("b", 20, cumulative=tokens(130, output=12, reasoning=6)),
        ]
        result = aggregate([self.jsonl(events)])
        self.assertEqual(result["status"], "observed")
        self.assertEqual(result["observed_usage"]["input_tokens"], 30)

    def test_independent_rollout_path_order_does_not_change_record_totals(self):
        first = self.jsonl([record("first", 10)], "a.jsonl")
        second = self.jsonl(
            [record("second", 20, cumulative=tokens(30, output=8, reasoning=4))],
            "b.jsonl",
        )
        forward = aggregate([first, second])
        backward = aggregate([second, first])
        self.assertEqual(forward, backward)

    def test_counter_root_filter_is_explicitly_unsupported(self):
        path = self.jsonl(
            [
                {"type": "thread.started", "thread_id": "root"},
                {"type": "turn.completed", "usage": tokens(10)},
            ]
        )
        self.assertEqual(aggregate([path], {"turn-root"})["status"], "invalid")

    def test_bad_json_cannot_be_silently_skipped(self):
        path = self.directory / "broken.jsonl"
        path.write_text("{bad json\n", encoding="utf-8")
        with self.assertRaisesRegex(ValueError, "broken.jsonl:1"):
            aggregate([path])


class FixtureAndRunnerTests(WorkspaceTest):
    def test_all_twelve_fixtures_are_deterministic_and_oracle_rejects_wrong_answers(
        self,
    ):
        self.assertEqual(len(CASES), 12)
        for case in CASES:
            first, second = self.directory / f"{case}-a", self.directory / f"{case}-b"
            expected = materialize(first, case)
            self.assertEqual(expected, materialize(second, case))
            self.assertFalse((first / "oracle.json").exists())
            self.assertFalse(check_answer(first, expected)["passed"])
            (first / "answer.json").write_text(
                json.dumps({"case": case}), encoding="utf-8"
            )
            self.assertFalse(check_answer(first, expected)["passed"])
            (first / "answer.json").write_text(
                json.dumps(expected["answer"]), encoding="utf-8"
            )
            self.assertTrue(check_answer(first, expected)["passed"])
        self.assertIn(
            b"\r\n", (self.directory / "unicode_crlf-a/stdout.bin").read_bytes()
        )
        self.assertEqual(
            (self.directory / "stderr_only-a/stdout.bin").read_bytes(), b""
        )
        self.assertGreater(
            len((self.directory / "long_json_line-a/stdout.bin").read_bytes()), 60000
        )

    def test_schedule_alternates_conditions(self):
        result = list(schedule(["task"], ["A", "B"], 3))
        self.assertEqual(
            [condition for _, condition, _ in result], ["A", "B", "B", "A", "A", "B"]
        )

    def test_offline_self_check_uses_real_local_io_and_no_model(self):
        manifest = load_manifest(
            Path("scripts/context_input_evals/fixtures/manifest.json")
        )
        with patch(
            "socket.create_connection",
            side_effect=AssertionError("offline must not connect"),
        ):
            report = self_check(manifest, self.directory / "self-check")
        self.assertEqual(len(report["attempts"]), 12)
        self.assertTrue(all(item["status"] == "passed" for item in report["attempts"]))
        self.assertTrue(
            all(item["usage"]["observed_usage"] is None for item in report["attempts"])
        )
        self.assertTrue((self.directory / "self-check/report.md").is_file())

    def test_global_home_and_missing_auth_fail_before_subprocess(self):
        manifest = load_manifest(
            Path("scripts/context_input_evals/fixtures/manifest.json")
        )
        import sys

        with patch(
            "subprocess.run", side_effect=AssertionError("must fail before execution")
        ):
            with self.assertRaisesRegex(ValueError, "isolated home"):
                run(
                    manifest,
                    [
                        {
                            "name": "A",
                            "executable": sys.executable,
                            "home": str(Path.home() / ".codex"),
                        }
                    ],
                    1,
                    self.directory / "global",
                    1,
                )
            with self.assertRaisesRegex(ValueError, "auth.json"):
                run(
                    manifest,
                    [
                        {
                            "name": "A",
                            "executable": sys.executable,
                            "home": str(self.directory / "empty-home"),
                        }
                    ],
                    1,
                    self.directory / "unauthenticated",
                    1,
                )

    def test_real_local_timeout_retains_output_and_stops_owned_process(self):
        import sys

        stdout, stderr, code, status = execute(
            [
                sys.executable,
                "-c",
                "import time; print('started', flush=True); time.sleep(60)",
            ],
            self.directory,
            None,
            0.3,
        )
        self.assertEqual(status, "timeout")
        self.assertIn(b"started", stdout)
        self.assertIsNotNone(code)

    def test_exec_root_scope_includes_child_and_excludes_other_thread(self):
        events = [
            record("parent", 10),
            record("child", 7, thread="child"),
            record("other", 50, thread="other", root="other-turn"),
        ]
        result, scope = scoped_usage(
            [self.jsonl(events)], b'{"type":"thread.started","thread_id":"root"}\n'
        )
        self.assertEqual(result["observed_usage"]["input_tokens"], 17)
        self.assertEqual(scope, {"root_thread": "root", "root_turn_ids": ["turn-root"]})

    def test_unknown_exec_root_cannot_attribute_usage(self):
        result, scope = scoped_usage([self.jsonl([record("parent", 10)])], b"")
        self.assertIsNone(result["observed_usage"])
        self.assertIsNone(scope["root_thread"])

    def test_configuration_gate_checks_effective_root_model_and_high(self):
        scope = {"root_thread": "root", "root_turn_ids": ["turn-root"]}
        manifest = {"model": "gpt-6.1-sol", "effort": "high"}
        metadata = {"type": "session_meta", "payload": {"id": "root"}}
        for model, effort, status in (
            ("gpt-6.1-sol", "high", "verified"),
            ("other", "high", "mismatch"),
            ("gpt-6.1-sol", "low", "mismatch"),
            ("gpt-6.1-sol", None, "missing"),
        ):
            with self.subTest(model=model, effort=effort):
                context = {
                    "type": "turn_context",
                    "payload": {
                        "turn_id": "turn-root",
                        "model": model,
                        "effort": effort,
                    },
                }
                result = verify_configuration(
                    [self.jsonl([metadata, context])], scope, manifest
                )
                self.assertEqual(result["status"], status)
        self.assertEqual(verify_configuration([], scope, manifest)["status"], "missing")

    def test_exited_root_does_not_leave_inherited_child_pipe_past_deadline(self):
        import sys
        import time

        started = time.perf_counter()
        command = [
            sys.executable,
            "-c",
            "import subprocess,sys; p=subprocess.Popen([sys.executable,'-c','import time; time.sleep(60)']); print('owned-child',p.pid,flush=True)",
        ]
        stdout, stderr, code, status = execute(command, self.directory, None, 0.15)
        self.assertEqual(status, "timeout")
        self.assertEqual(code, 0)
        self.assertIn(b"owned-child", stdout)
        self.assertLess(time.perf_counter() - started, 1.5)

    def test_isolated_environment_removes_all_supported_auth_overrides(self):
        import os

        synthetic = {
            "OPENAI_API_KEY": "synthetic-openai",
            "CODEX_API_KEY": "synthetic-codex",
            "CODEX_ACCESS_TOKEN": "synthetic-token",
            "OPENAI_FEDERATION_RULE_ID": "synthetic-rule",
            "OPENAI_IDENTITY_TOKEN_FILE": "synthetic-token-path",
            "OPENAI_WORKLOAD_IDENTITY_CONTEXT": "synthetic-context",
        }
        with patch.dict(os.environ, synthetic):
            environment = isolated_environment(self.directory)
            for name in synthetic:
                self.assertNotIn(name, environment)
                self.assertEqual(os.environ[name], synthetic[name])
            self.assertEqual(environment["CODEX_HOME"], str(self.directory.resolve()))

    def test_isolated_environment_removes_case_variants_without_changing_parent(self):
        synthetic = {
            "openai_api_key": "synthetic-openai",
            "CodeX_Api_Key": "synthetic-codex",
            "codex_access_token": "synthetic-token",
            "openai_federation_rule_id": "synthetic-rule",
            "OpenAI_Identity_Token_File": "synthetic-token-path",
            "OPENAI_workload_IDENTITY_context": "synthetic-context",
            "codex_home": "synthetic-old-home",
            "UNCHANGED": "synthetic-unchanged",
        }
        original = dict(synthetic)
        with patch("scripts.context_input_evals.runner.os.environ", synthetic):
            environment = isolated_environment(self.directory)
        self.assertEqual(synthetic, original)
        self.assertEqual(
            environment,
            {
                "CODEX_HOME": str(self.directory.resolve()),
                "UNCHANGED": "synthetic-unchanged",
            },
        )

    def test_oracle_marks_utf16_and_invalid_utf8_answer_files_failed(self):
        workspace = self.directory / "answer-encoding"
        expected = materialize(workspace, "stderr_only")
        for content in (
            json.dumps(expected["answer"]).encode("utf-16"),
            b'{"case":"\xff"}',
        ):
            with self.subTest(content=content[:2]):
                (workspace / "answer.json").write_bytes(content)
                result = check_answer(workspace, expected)
                self.assertFalse(result["passed"])
                self.assertIn("answer.json is not valid UTF-8", result["reason"])

    def test_model_boundary_bad_answer_retains_cost_and_continues_pair(self):
        import os
        import sys

        manifest = load_manifest(
            Path("scripts/context_input_evals/fixtures/manifest.json")
        )
        manifest["tasks"] = ["stderr_only"]
        markers = {
            "OPENAI_API_KEY": "synthetic-openai",
            "CODEX_API_KEY": "synthetic-codex",
            "CODEX_ACCESS_TOKEN": "synthetic-token",
            "OPENAI_FEDERATION_RULE_ID": "synthetic-rule",
            "OPENAI_IDENTITY_TOKEN_FILE": "synthetic-token-path",
            "OPENAI_WORKLOAD_IDENTITY_CONTEXT": "synthetic-context",
        }
        for encoding in ("utf16", "invalid_utf8"):
            with self.subTest(encoding=encoding):
                conditions = []
                for name in ("baseline", "candidate"):
                    home = self.directory / f"{encoding}-{name}-home"
                    home.mkdir()
                    (home / "auth.json").write_text("{}", encoding="utf-8")
                    conditions.append(
                        {"name": name, "executable": sys.executable, "home": str(home)}
                    )

                # Isolate only the model execution boundary. Fixture files,
                # malformed answer bytes, rollout records and reports are real.
                def model_boundary(command, cwd, environment, timeout):
                    self.assertTrue(
                        all(name.upper() not in markers for name in environment)
                    )
                    if command[-1] == "--version":
                        return b"codex synthetic-boundary\n", b"", 0, "completed"
                    workspace = Path(cwd)
                    is_baseline = "baseline" in workspace.name
                    name = "baseline" if is_baseline else "candidate"
                    thread, turn = f"thread-{name}", f"turn-{name}"
                    evidence = (
                        (workspace / "stderr.bin")
                        .read_text(encoding="utf-8")
                        .split()[1]
                    )
                    answer = {
                        "case": "stderr_only",
                        "status": "failed",
                        "evidence": evidence,
                    }
                    answer_bytes = json.dumps(answer).encode("utf-8")
                    if is_baseline:
                        answer_bytes = (
                            json.dumps(answer).encode("utf-16")
                            if encoding == "utf16"
                            else b'{"case":"\xff"}'
                        )
                    (workspace / "answer.json").write_bytes(answer_bytes)
                    events = [
                        {"type": "session_meta", "payload": {"id": thread}},
                        {
                            "type": "turn_context",
                            "payload": {
                                "turn_id": turn,
                                "model": manifest["model"],
                                "effort": "high",
                            },
                        },
                        record(
                            f"response-{name}",
                            10 if is_baseline else 20,
                            thread=thread,
                            root=turn,
                            turn=turn,
                        ),
                    ]
                    session_dir = Path(environment["CODEX_HOME"]) / "sessions"
                    session_dir.mkdir()
                    (session_dir / "rollout.jsonl").write_text(
                        "".join(json.dumps(event) + "\n" for event in events),
                        encoding="utf-8",
                    )
                    stdout = (
                        json.dumps({"type": "thread.started", "thread_id": thread})
                        + "\n"
                    ).encode()
                    return stdout, b"", 0, "completed"

                run_dir = self.directory / f"{encoding}-run"
                with (
                    patch.dict(os.environ, markers),
                    patch(
                        "scripts.context_input_evals.runner.execute",
                        side_effect=model_boundary,
                    ) as boundary,
                ):
                    report = run(manifest, conditions, 1, run_dir, 1)
                    self.assertTrue(
                        all(
                            os.environ[name] == value for name, value in markers.items()
                        )
                    )
                self.assertEqual(
                    boundary.call_count, 4
                )  # Two versions, two model boundaries.
                self.assertEqual(
                    [attempt["status"] for attempt in report["attempts"]],
                    ["failed", "passed"],
                )
                failed = report["attempts"][0]
                self.assertEqual(failed["usage"]["observed_usage"]["input_tokens"], 10)
                self.assertIn("not valid UTF-8", failed["oracle"]["reason"])
                self.assertEqual(
                    report["summary"]["conditions"]["baseline"][
                        "observed_input_tokens"
                    ],
                    10,
                )
                persisted = json.loads(
                    (run_dir / "report.json").read_text(encoding="utf-8")
                )
                self.assertEqual(persisted["attempts"], report["attempts"])
                self.assertEqual(len(persisted["attempts"]), 2)

    def test_counted_child_without_effective_context_keeps_gate_missing(self):
        scope = {"root_thread": "root", "root_turn_ids": ["turn-root"]}
        manifest = {"model": "gpt-6.1-sol", "effort": "high"}
        root = [
            {"type": "session_meta", "payload": {"id": "root"}},
            {
                "type": "turn_context",
                "payload": {
                    "turn_id": "turn-root",
                    "model": "gpt-6.1-sol",
                    "effort": "high",
                },
            },
            record("parent", 10),
            record("child", 7, thread="child"),
        ]
        path = self.jsonl(root)
        self.assertEqual(
            verify_configuration([path], scope, manifest)["status"], "missing"
        )
        root += [
            {"type": "session_meta", "payload": {"id": "child"}},
            {
                "type": "turn_context",
                "payload": {
                    "turn_id": "turn-root",
                    "root_turn_id": "turn-root",
                    "model": "gpt-6.1-sol",
                    "effort": "high",
                },
            },
        ]
        self.assertEqual(
            verify_configuration([self.jsonl(root)], scope, manifest)["status"],
            "verified",
        )

    def test_model_profile_and_dotted_alias_overrides_fail_before_model_run(self):
        import sys

        home = self.directory / "dedicated-home"
        home.mkdir()
        (home / "auth.json").write_text("{}", encoding="utf-8")
        config = self.directory / "config.json"
        manifest = load_manifest(
            Path("scripts/context_input_evals/fixtures/manifest.json")
        )
        for key in (
            "model",
            "model_provider",
            "profiles.work.model",
            "profile",
            "agents.worker.reasoning_effort",
            "collaboration_mode",
        ):
            with self.subTest(key=key):
                config.write_text(json.dumps({key: "bad"}), encoding="utf-8")
                with patch(
                    "subprocess.run",
                    side_effect=AssertionError(
                        "preflight must reject config before invoking client"
                    ),
                ):
                    with self.assertRaisesRegex(ValueError, "config overrides"):
                        run(
                            manifest,
                            [
                                {
                                    "name": "A",
                                    "home": str(home),
                                    "executable": sys.executable,
                                    "config": str(config),
                                }
                            ],
                            1,
                            self.directory / "run",
                            1,
                        )

    def test_manifest_requires_explicit_model_high_and_known_tasks(self):
        manifest = load_manifest(
            Path("scripts/context_input_evals/fixtures/manifest.json")
        )
        for field, value in (("effort", "low"), ("model", ""), ("tasks", ["unknown"])):
            with self.subTest(field=field):
                path = self.directory / "manifest.json"
                path.write_text(
                    json.dumps(dict(manifest, **{field: value})), encoding="utf-8"
                )
                with self.assertRaises(ValueError):
                    load_manifest(path)


class ReportTests(WorkspaceTest):
    def attempt(self, status, number, condition="baseline", repeat=0):
        usage = {
            "status": "observed" if number is not None else "missing",
            "observed_usage": tokens(number) if number is not None else None,
            "complete_total": None,
        }
        return {
            "task": "middle_error",
            "condition": condition,
            "repeat": repeat,
            "status": status,
            "usage": usage,
            "wall_seconds": 2.0,
        }

    def test_failed_timeout_and_cancelled_costs_are_retained(self):
        rows = [
            self.attempt(status, number)
            for status, number in (
                ("passed", 10),
                ("failed", 20),
                ("timeout", 30),
                ("cancelled", 40),
                ("failed", None),
            )
        ]
        summary = summarize(rows)["conditions"]["baseline"]
        self.assertEqual(summary["attempts"], 5)
        self.assertEqual(summary["successes"], 1)
        self.assertEqual(summary["observed_input_tokens"], 100)
        self.assertEqual(summary["observed_input_mean"], 25)
        self.assertEqual(summary["usage_observed_attempts"], 4)
        self.assertIsNone(summary["input_per_success"])

    def test_pairs_require_both_observed_and_do_not_hide_failure(self):
        summary = summarize(
            [self.attempt("passed", 10), self.attempt("failed", 6, "candidate")]
        )
        self.assertEqual(
            summary["paired_differences"][0]["observed_input_b_minus_a"], -4
        )
        self.assertFalse(summary["paired_differences"][0]["both_passed"])
        self.assertEqual(
            summarize(
                [self.attempt("passed", None), self.attempt("passed", 6, "candidate")]
            )["paired_differences"],
            [],
        )

    def test_report_contains_no_raw_payload(self):
        rows = [self.attempt("failed", 10)]
        report = {"summary": summarize(rows), "attempts": rows}
        write_reports(self.directory, report)
        self.assertEqual(
            json.loads((self.directory / "report.json").read_text(encoding="utf-8"))[
                "summary"
            ],
            report["summary"],
        )
        self.assertIn(
            "Cached input is part of input",
            (self.directory / "report.md").read_text(encoding="utf-8"),
        )


if __name__ == "__main__":
    unittest.main()
