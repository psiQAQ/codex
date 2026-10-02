# Recoverable local exec output

Recoverable output is an opt-in evidence cache for ordinary local, non-PTY pipe commands. It captures stdout and stderr before unified exec's lossy merge and 1 MiB head/tail buffers. The default is disabled.

Enable it in your configuration:

```toml
[tools.recoverable_exec_output]
enabled = true
artifact_max_bytes = 8388608
session_max_bytes = 134217728
global_max_bytes = 1073741824
ttl_seconds = 86400
preview_max_tokens = 400
```

The artifact limit covers both streams together. The session limit is shared by a root thread and its descendants; lookup authority remains specific to the producing thread and its selected environment. Each run has a fresh opaque artifact ID and run ID, independent of repeated tool call IDs or process IDs.

The cache lives under Codex home in `recoverable-exec-output`, outside the working tree. Each admitted artifact conservatively reserves its full raw limit plus 4096 bytes for metadata. This keeps concurrent-process accounting bounded, even for running commands; small outputs may therefore reach the configured admission limit before their physical files fill the disk allowance. Admission uses a cross-process file lock and fails back to ordinary output instead of waiting on lock contention. Active readers and writers hold cleanup leases. Expired objects reject new lookups; their files are removed during later cache admission when no lease is active.

Lookup references are valid only while the producing Codex session retains the artifact registry. Restarted or resumed sessions do not reconstruct old references from disk; those cache files remain subject to quota accounting and expiry cleanup.

Raw bytes remain separate for stdout and stderr. Receipts report observed, stored, and omitted byte counts and SHA-256 digests for each stream. Digests are final only when the command has completed, the stream has closed, and queued writes have finished; cancellation and timeout receipts retain nonfinal digests. Combined previews reflect the collector's receive order, which does not establish a strict chronological order between two independent operating-system streams.

`read_exec_output` returns bounded original text fragments by one-based line number. `search_exec_output` performs literal single-line matching with optional context. Both require an artifact ID from the current thread and a selected environment. They do not accept filesystem paths. Use the returned opaque `next_cursor` with the same artifact, stream, and query to continue a committed snapshot; consumed cursors cannot be reused. New output requires a fresh lookup.

Lookups scan at most 256 KiB per call and return at most 8000 serialized UTF-8 bytes, including receipt metadata. Queries are limited to 512 bytes, reads to 100 fragments, matches to 50, and context to five fragments. Long lines are split into bounded fragments with `continued=true`; repeated line numbers identify fragments of the same line. Literal searches retain a bounded overlap so matches can cross fragment boundaries. A snapshot that ends inside a still-incomplete UTF-8 character reports its trailing incomplete bytes instead of inventing replacement characters.

UTF-8 text is supported, including CRLF and files without a final newline. Invalid UTF-8 and NUL-containing output produce an explicit unsupported-encoding fallback. PTY, remote or local exec-server execution, and Windows restricted-token driver execution are unsupported and retain the existing bounded output path. This feature does not cover every local execution backend.

An admitted capture uses a bounded queue. Disk errors, queue exhaustion, permission failures, quota exhaustion, cancellation, or expiry cannot change the original command, approval, sandbox retry, or exit code. A partial artifact identifies the retained prefix and omitted bytes; an unavailable artifact has no valid lookup reference. Output optimization failures do not become command failures.

Core tool results keep their receipt as a valid structured envelope even with a tiny preview budget. The history budget reserves the serialized envelope. Code mode receives the same structured result, so JavaScript can retain the object without parsing a truncated JSON preview. Code mode's outer printing budget still applies when that object is printed or returned through another wrapper: a tiny outer budget can truncate its displayed reference. The outer code-mode channel is outside this feature's scope.

On Unix the cache directory is owner-only and newly created regular files use mode 0600. On Windows the directory inherits the configured Codex home's ACL. Cache opens refuse symlinks and Windows reparse points, including intermediate components, and creation is exclusive. Opaque IDs prevent lookup tools from becoming arbitrary file readers; they do not isolate the cache from other processes running as the same user. Shell visibility still follows the configured sandbox filesystem policy. This feature does not expand that policy or claim that the cache path is inaccessible to an otherwise-authorized shell.

Receipts and references remain small in rollout history. Raw cache contents are not added to rollout or public telemetry. No token-saving percentage is promised; recovery correctness and input-token effects must be measured separately.
