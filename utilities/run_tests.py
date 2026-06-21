import os
import subprocess
import sys

try:
    from bootstrap import ensure_repo_on_path
except ModuleNotFoundError:
    from utilities.bootstrap import ensure_repo_on_path

repo_root = ensure_repo_on_path()


def _python_exec():
    venv_python = repo_root / ".venv" / "bin" / "python3"
    if venv_python.exists():
        return str(venv_python)

    return sys.executable


def main():
    env = os.environ.copy()
    env["PYTHONPYCACHEPREFIX"] = "/tmp/arbivision-pyc"
    env["PYTHONASYNCIODEBUG"] = "0"
    env["PYTHONPATH"] = str(repo_root)
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

    result = subprocess.run(cmd, env=env, cwd=repo_root)
    raise SystemExit(result.returncode)


if __name__ == "__main__":
    main()
