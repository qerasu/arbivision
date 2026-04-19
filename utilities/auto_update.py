import os
import subprocess
import sys
from pathlib import Path

try:
    from bootstrap import repo_root
except ModuleNotFoundError:
    from utilities.bootstrap import repo_root


TARGET_BRANCH = "main"


def _python_exec():
    root = repo_root()
    if os.name == "nt":
        candidates = [
            root / ".venv" / "Scripts" / "python.exe",
            root / ".venv" / "Scripts" / "python",
        ]
    else:
        candidates = [
            root / ".venv" / "bin" / "python3",
            root / ".venv" / "bin" / "python",
        ]

    for candidate in candidates:
        if candidate.exists():
            return str(candidate)

    return sys.executable


def _run(cmd):
    print(f"running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=repo_root(), text=True)
    if result.returncode != 0:
        raise SystemExit(result.returncode)
    return result


def _capture(cmd):
    result = subprocess.run(
        cmd,
        cwd=repo_root(),
        text=True,
        capture_output=True,
    )
    if result.returncode != 0:
        stderr = result.stderr.strip()
        if stderr:
            print(stderr)
        raise SystemExit(result.returncode)
    return result.stdout.strip()


def _head_sha(ref):
    return _capture(["git", "rev-parse", ref])


def _start_service_detached(python_exec):
    start_script = repo_root() / "utilities" / "start.py"
    cmd = [python_exec, str(start_script)]
    creationflags = 0
    popen_kwargs = {
        "cwd": str(repo_root()),
        "stdin": subprocess.DEVNULL,
        "stdout": subprocess.DEVNULL,
        "stderr": subprocess.DEVNULL,
        "close_fds": True,
    }

    if os.name == "nt":
        creationflags = subprocess.CREATE_NEW_PROCESS_GROUP | subprocess.DETACHED_PROCESS
    else:
        popen_kwargs["start_new_session"] = True

    print(f"running detached: {' '.join(cmd)}")
    subprocess.Popen(
        cmd,
        creationflags=creationflags,
        **popen_kwargs,
    )


def main():
    python_exec = _python_exec()

    _run(["git", "fetch", "origin", TARGET_BRANCH])

    local_head = _head_sha("HEAD")
    remote_head = _head_sha(f"origin/{TARGET_BRANCH}")

    print(f"local HEAD: {local_head}")
    print(f"remote HEAD: {remote_head}")

    if local_head == remote_head:
        print("no updates found")
        return

    print("updates found, restarting service")
    _run([python_exec, "utilities/stop.py"])
    _run(["git", "pull", "--ff-only", "origin", TARGET_BRANCH])
    _start_service_detached(python_exec)
    print("update completed")


if __name__ == "__main__":
    main()