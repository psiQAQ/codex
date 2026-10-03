import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import unittest


class WindowsVoiceBazelEnvironmentTest(unittest.TestCase):
    def invoke(self, runner_os, *, remote=False, system_root="C:/Windows"):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            wrapper = root / "run-bazel-ci.sh"
            shutil.copyfile(Path(__file__).with_name(wrapper.name), wrapper)
            driver = root / "run_bazel_with_buildbuddy.py"
            driver.write_text(
                '#!/usr/bin/env bash\nprintf "%s\\n" "$@" > "$CAPTURE_ARGS"\n',
                encoding="utf-8",
            )
            driver.chmod(0o755)
            capture = root / "args.txt"
            env = os.environ.copy()
            for key in tuple(env):
                if key.startswith(
                    ("BAZEL_", "CODEX_BAZEL_", "BUILDBUDDY_", "VOICE_WINDOWS_")
                ):
                    env.pop(key)
            env.update(
                {
                    "RUNNER_OS": runner_os,
                    "CODEX_BAZEL_WINDOWS_PATH": "C:/Windows/System32",
                    "VOICE_WINDOWS_BAZEL_REPOSITORY": "D:/voice tools",
                    "VOICE_WINDOWS_SYSTEM_ROOT": system_root,
                    "VOICE_WINDOWS_HOST_ARCH": "AMD64",
                    "CAPTURE_ARGS": capture.as_posix(),
                    "TMPDIR": root.as_posix(),
                    "BUILDBUDDY_API_KEY": "test-key" if remote else "",
                }
            )
            bash = (
                "C:/Program Files/Git/bin/bash.exe"
                if os.name == "nt"
                else shutil.which("bash")
            )
            result = subprocess.run(
                [
                    bash,
                    str(wrapper),
                    "--windows-cross-compile",
                    "--",
                    "build",
                    "--",
                    "//codex-rs/voice-host:codex-voice-host",
                ],
                env=env,
                capture_output=True,
                text=True,
            )
            args = (
                capture.read_text(encoding="utf-8").splitlines()
                if capture.exists()
                else []
            )
            return result, args

    def test_native_windows_voice_inputs_reach_bazel_analysis(self):
        result, args = self.invoke("Windows")
        self.assertEqual(result.returncode, 0, result.stderr)
        for expected in (
            "--inject_repository=voice_windows_tools=D:/voice tools",
            "--//third_party/voice:windows_installed_tools=@voice_windows_tools//:tools",
            "--action_env=SystemRoot=C:/Windows",
            "--host_action_env=SystemRoot=C:/Windows",
            "--action_env=PROCESSOR_ARCHITECTURE=AMD64",
            "--host_action_env=PROCESSOR_ARCHITECTURE=AMD64",
        ):
            self.assertIn(expected, args)

    def test_native_windows_missing_system_root_fails_before_bazel(self):
        result, args = self.invoke("Windows", system_root="")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("VOICE_WINDOWS_SYSTEM_ROOT", result.stderr)
        self.assertEqual(args, [])

    def test_linux_actions_do_not_receive_windows_voice_inputs(self):
        for runner_os, remote in (("Linux", False), ("Windows", True)):
            with self.subTest(runner_os=runner_os, remote=remote):
                result, args = self.invoke(runner_os, remote=remote)
                self.assertEqual(result.returncode, 0, result.stderr)
                self.assertFalse(
                    any(
                        "voice_windows_tools" in arg
                        or "SystemRoot=" in arg
                        or "PROCESSOR_ARCHITECTURE=" in arg
                        for arg in args
                    )
                )


if __name__ == "__main__":
    unittest.main()
