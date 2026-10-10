# Context input evaluations

This standard-library harness measures observed model usage and structured task
results for independently built Codex binaries. It does not import candidate
features or modify core, dependencies, authentication, or the global Codex home.

Run from the repository root with Python 3.11 or newer:

```powershell
python -m unittest discover -s scripts/context_input_evals/tests -v
python -m scripts.context_input_evals self-check --run-dir .context-dev/e0-self-check
python -m scripts.context_input_evals analyze path/to/rollout.jsonl
```

The run directory must be new. The twelve deterministic fixtures cover middle
errors, multiple failures, empty stdout, stderr, long JSON, Unicode/CRLF,
cancelled status, quota status, expiry, repetition, missing metadata, and
cross-session denial. Offline checks exercise local subprocess bytes, fixture
generation, oracles, and accounting. Lifecycle cases supply synthetic status
data; they do not establish actual artifact quota/expiry/session enforcement.
Model success and candidate feature behavior need separate integration runs.
The oracle requires UTF-8 JSON. Missing, malformed or non-UTF-8 answers fail that
attempt, retain its observed usage in reports, and allow later paired attempts
to continue. An answer encoding failure is never treated as zero model cost.

## Measurement contract

- Rollout `token_usage_record.usage` is per upstream completion. Deduplicate by
  `(thread_id, response_id)` across files; include different retry responses and
  child threads. `--root-turn-id` selects causal root turns, including children.
- Validate `turn_token_usage` and `thread_token_usage` snapshots against records;
  permit a constant prior-history prefix but never sum snapshots as requests.
- Ignore `token_count` and exec snapshots for a thread with response records.
  Counter-only streams preserve monotonicity within each file, then select
  the latest comparable endpoint per thread across duplicate/overlapping files,
  including prior history. File argument order does not change totals. A decreasing counter or conflicting record makes usage invalid.
  Counter-only root selection is unsupported and reported as invalid.
- Missing usage stays `null`. Required fields are never guessed from bytes or
  filled with zero. Cached input is part of input; reasoning output is part of
  output. Neither subset is added twice.
- Usage remains **observed**, with `complete_total: null`: a provider may omit
  usage, particularly for failed requests. Actual billable task totals, monetary
  costs, and subscription quota costs cannot be inferred from these records.
- Reports retain all failed, timed-out and cancelled attempts and their observed
  usage. Means include failed attempts with observed usage and report coverage.
  `input_per_success` stays null because complete attempt costs are unavailable.
  Paired differences cover two observed conditions for the same task/repeat;
  neither success-rate noninferiority nor savings is asserted.
- `codex exec --json` currently exposes thread cumulative usage on completed
  turns and no usage on failed turns. Explicit runs collect root and child
  rollouts from a supplied isolated home, then select the actual root thread
  and its causal root turns, including child records, instead of treating exec
  as per-call accounting. The adapter matches current protocol records and tests.

## Explicit model runs

Online runs consume model allowance and are never called by CI or `self-check`.
Supply already authenticated, dedicated, unused homes for each condition; the
runner refuses the current/global home, missing `auth.json`, and existing
rollouts. It does not copy credentials. Child environments remove inherited
`OPENAI_API_KEY`, `CODEX_API_KEY`, `CODEX_ACCESS_TOKEN`,
`OPENAI_FEDERATION_RULE_ID`, `OPENAI_IDENTITY_TOKEN_FILE` and
`OPENAI_WORKLOAD_IDENTITY_CONTEXT`. Comparison is case-insensitive to honor
Windows environment semantics; competing `CODEX_HOME` spellings are replaced
with the supplied isolated home. Only the child environment copy is changed.
These variables cannot select a different account or activate workload identity.
Use the same binary version, model,
instructions, task seed, and high effort across conditions. Override the model
in a reviewed task manifest if needed; no automatic model fallback occurs.

Create a private JSON conditions file inside the ignored task run area:

```json
[
  {"name": "baseline", "executable": "D:/build/baseline/codex.exe", "home": "D:/eval-homes/baseline", "cache_stratum": "unspecified"},
  {"name": "candidate", "executable": "D:/build/candidate/codex.exe", "home": "D:/eval-homes/candidate", "config": "D:/eval-input/candidate-overrides.json", "cache_stratum": "unspecified"}
]
```

Optional condition config is a JSON map of explicit Codex `-c` overrides,
for example `{"tool_output_token_limit": 1000}`. Values are strings, integers
or booleans. Model, effort and provider overrides are rejected so the manifest
remains authoritative. SHA-256 and client version are collected before tasks run.
The command uses `--ignore-user-config` and `--strict-config`; condition overrides
cannot select model/profile/provider or collaboration settings, including dotted
aliases. A completed attempt passes only when its root rollout `turn_context`
verifies the requested model and high effort. Every counted root/child usage
turn must have matching effective turn-context evidence; missing child context
keeps the gate missing. Missing/mismatched configuration or observed model rerouting keeps
the attempt unsuccessful and preserves its observed usage. Deadline cleanup
terminates the owned POSIX process group or Windows Job Object, including children
after the root exits. Windows starts the root suspended and assigns its job before
resuming it; the final output-pipe drain is limited to one second. Unsupported job
assignment fails before the root starts. The Windows behavior follows documented
[job membership](https://learn.microsoft.com/en-us/windows/win32/api/jobapi2/nf-jobapi2-assignprocesstojobobject)
and [job termination](https://learn.microsoft.com/en-us/windows/win32/api/jobapi2/nf-jobapi2-terminatejobobject).
Offline tests verify
this gate, not actual online model execution.

```powershell
python -m scripts.context_input_evals run --manifest scripts/context_input_evals/fixtures/manifest.json --conditions .context-dev/conditions.json --repeats 2 --run-dir .context-dev/e0-online
```

Order alternates A/B and B/A between repetitions. Each attempt gets a fresh
fixture workspace; service start timestamps and declared cache strata are
recorded. A local fresh workspace cannot guarantee a service-side cold cache.
The initial synthetic tasks check the CLI/measurement chain by asking for a
structured evidence answer checked against an oracle held outside the model workspace; useful efficiency experiments require preregistered
tasks that independently exercise the candidate feature.

`report.json` and `report.md` contain synthetic task IDs, aggregate metrics and
manifest hashes. Private raw exec output and rollouts are never embedded in
reports. Keep real prompts, global AGENTS, tool output and authentication out
of public commits. Default output belongs under the task's ignored
`.context-dev/` directory. Preserve run evidence until review; delete only
identified task-generated directories afterwards.
