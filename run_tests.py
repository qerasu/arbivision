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

    cmd = [
        _python_exec(),
        "-m",
        "unittest",
        "discover",
        "-s",
        "tests",
        "-v",
    ]

    result = subprocess.run(cmd, env=env)
    raise SystemExit(result.returncode)

if __name__ == "__main__":
    main()