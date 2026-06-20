import argparse
import os
import signal
import subprocess
import tempfile
from pathlib import Path
import time

try:
    from bootstrap import ensure_repo_on_path, env_file_path
except ModuleNotFoundError:
    from utilities.bootstrap import ensure_repo_on_path, env_file_path

ensure_repo_on_path()

from arbitrage_bot.core.env_loader import load_env_file

ENV_FILE_PATH = env_file_path()


def _pidfile():
    return Path(tempfile.gettempdir()) / "arbitrage_alert_bot.pid"


def _wait_for_exit(pid, timeout=6):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            os.kill(pid, 0)
            time.sleep(0.2)
        except ProcessLookupError:
            return True
        except OSError:
            # treat permission errors as alive to avoid endless loops
            return False
    return False


def _kill_process_group(pid, signum):
    try:
        os.killpg(os.getpgid(pid), signum)
        return True
    except ProcessLookupError:
        return False
    except OSError:
        try:
            os.kill(pid, signum)
            return True
        except ProcessLookupError:
            return False


def _is_tracked_uvicorn_process(pid):
    try:
        result = subprocess.run(
            ["ps", "-p", str(pid), "-o", "command="],
            capture_output=True,
            text=True,
        )
    except FileNotFoundError:
        print("warning: ps is unavailable; refusing to stop tracked process")
        return False

    command = result.stdout.strip()
    return result.returncode == 0 and "uvicorn" in command and "arbitrage_bot.main:app" in command


def _stop_project_containers(drop_db=False):
    if drop_db:
        cmd = ["docker", "compose", "down", "-v"]
        success_msg = "postgres and redis data were removed successfully"
    else:
        cmd = ["docker", "compose", "stop"]
        success_msg = ""

    display_cmd = " ".join(cmd)
    print(f"running: {display_cmd}")
    result = subprocess.run(cmd, cwd=Path(__file__).resolve().parent.parent)
    if result.returncode != 0:
        print(f"error while running: {display_cmd}")
        return False

    if success_msg:
        print(f"\n{success_msg}")
    return True


def _confirm_drop(force):
    if force:
        return

    print("\nWARNING: this will remove docker containers, network, and volumes for Postgres and Redis.")
    print("WARNING: all current data in these databases will be deleted.\n")
    answer = input("type 'drop' to continue: ").strip().lower()
    if answer != "drop":
        print("operation cancelled")
        raise SystemExit(1)


def _parse_args():
    parser = argparse.ArgumentParser(description="Stop the arbitrage bot.")
    parser.add_argument(
        "--drop",
        action="store_true",
        help="Drop databases (remove containers, network, and volumes)",
    )
    parser.add_argument(
        "--yes",
        action="store_true",
        help="Skip confirmation prompt when using --drop",
    )
    return parser.parse_args()


def _is_port_in_use(host, port):
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((host, port))
            return False
        except OSError:
            return True


def main():
    args = _parse_args()
    
    if args.drop:
        _confirm_drop(args.yes)

    # load environment only from the shared config path
    load_env_file(str(ENV_FILE_PATH))

    if args.drop:
        print("=== dropping arbitrage alert bot databases ===")
    else:
        print("=== stopping arbitrage alert bot ===")
    app_host = os.environ.get("APP_HOST", "127.0.0.1")
    app_port = os.environ.get("APP_PORT", "8000")
    try:
        app_port_int = int(app_port)
    except (TypeError, ValueError):
        app_port_int = 8000

    # stop only the tracked uvicorn process to avoid killing unrelated services
    pid_file = _pidfile()
    if pid_file.exists():
        raw_pid = pid_file.read_text().strip()
        try:
            pid = int(raw_pid)
            if not _is_tracked_uvicorn_process(pid):
                print(f"tracked pid {pid} does not look like this project's uvicorn process; not stopping it")
            else:
                print(f"stopping tracked process {pid}...")
                try:
                    _kill_process_group(pid, signal.SIGTERM)
                    if not _wait_for_exit(pid):
                        _kill_process_group(pid, signal.SIGKILL)
                except ProcessLookupError:
                    print("tracked process not found; maybe already stopped")
                except Exception as exc:
                    print(f"failed to stop tracked process: {exc}")
        except ValueError:
            print(f"pid file contains invalid value: {raw_pid!r}")
        pid_file.unlink(missing_ok=True)
    else:
        print("pid file not found; no tracked process to stop")
        print("safe mode is enabled, so unrelated uvicorn processes were not touched")

    if _is_port_in_use(app_host, app_port_int):
        print(
            f"WARNING: port {app_host}:{app_port_int} is still busy. "
        )

    _stop_project_containers(drop_db=args.drop)

    print("bot was successfully stopped!")


if __name__ == "__main__":
    main()
