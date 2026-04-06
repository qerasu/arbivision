import os
import subprocess
import sys
from pathlib import Path


def _python_exec():
    venv_python = Path(".venv/bin/python3")
    if venv_python.exists():
        return str(venv_python)

    return sys.executable


def main():
    env = os.environ.copy()
    env["PYTHONPYCACHEPREFIX"] = "/tmp/arbivision-pyc"
    env["PYTHONASYNCIODEBUG"] = "0"
    env.pop("PYTHONDEVMODE", None)
    verbose = any(arg in {"-v", "--verbose"} for arg in sys.argv[1:])
    no_buffer = any(arg == "--no-buffer" for arg in sys.argv[1:])

    cmd = [
        _python_exec(),
        "-m",
        "unittest",
        "discover",
        "-s",
        "tests",
    ]
    if not verbose:
        cmd.append("-q")
    if not no_buffer:
        cmd.append("-b")
    if verbose:
        cmd.append("-v")

    result = subprocess.run(cmd, env=env)
    raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()