import os
import signal
import subprocess
import tempfile
from pathlib import Path
import time

ENV_FILE_PATH = Path.home() / ".config" / "arbivision" / ".env"


def _load_env_file(path):
    if not path.exists():
        return

    with path.open("r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue

            if "=" not in line:
                continue

            key, val = line.split("=", 1)
            key = key.strip().removeprefix("export ").strip()
            val = val.strip()
            if not key:
                continue

            if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
                val = val[1:-1]

            os.environ[key] = val


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


def _stop_project_containers():
    cmd = "docker compose stop"
    print(f"running: {cmd}")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        print(f"error while running: {cmd}")
        return False
    return True


def main():
    # load environment only from the shared config path
    _load_env_file(ENV_FILE_PATH)

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

    _stop_project_containers()

    print("bot was successfully stopped!")


def _is_port_in_use(host, port):
    import socket

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((host, port))
            return False
        except OSError:
            return True

if __name__ == "__main__":
    main()