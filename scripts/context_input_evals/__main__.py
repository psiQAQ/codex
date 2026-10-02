import argparse
import json
from pathlib import Path

from .runner import load_manifest, run, self_check
from .usage import aggregate


def main():
    parser = argparse.ArgumentParser(
        description="Offline context input checks and explicit black-box Codex runs"
    )
    commands = parser.add_subparsers(dest="command", required=True)
    analyze = commands.add_parser(
        "analyze", help="read existing rollout JSONL; no model invocation"
    )
    analyze.add_argument("paths", nargs="+")
    analyze.add_argument("--root-turn-id", action="append")
    for name in ("self-check", "run"):
        command = commands.add_parser(name)
        command.add_argument(
            "--manifest",
            default=str(Path(__file__).with_name("fixtures") / "manifest.json"),
        )
        command.add_argument("--run-dir", required=True)
        if name == "run":
            command.add_argument(
                "--conditions",
                required=True,
                help="JSON array: name, executable, home, optional config JSON",
            )
            command.add_argument("--repeats", type=int, default=2)
            command.add_argument("--timeout", type=float, default=300)
    arguments = parser.parse_args()
    try:
        if arguments.command == "analyze":
            result = aggregate(
                arguments.paths,
                set(arguments.root_turn_id) if arguments.root_turn_id else None,
            )
        else:
            manifest = load_manifest(arguments.manifest)
            if arguments.command == "self-check":
                result = self_check(manifest, arguments.run_dir)
            else:
                conditions = json.loads(
                    Path(arguments.conditions).read_text(encoding="utf-8")
                )
                result = run(
                    manifest,
                    conditions,
                    arguments.repeats,
                    arguments.run_dir,
                    arguments.timeout,
                )
    except (ValueError, OSError, KeyError) as error:
        parser.error(str(error))
    if arguments.command == "analyze":
        print(json.dumps(result, indent=2))
        return 1 if result["status"] == "invalid" else 0
    print(json.dumps(result["summary"], indent=2))
    return (
        0 if all(attempt["status"] == "passed" for attempt in result["attempts"]) else 1
    )


if __name__ == "__main__":
    raise SystemExit(main())
