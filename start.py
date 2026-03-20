import os
import subprocess
import sys
import signal
import tempfile
from pathlib import Path
import time
import socket

ENV_FILE_PATH = Path.home() / ".config" / "arbivision" / ".env"


def run_cmd(cmd):
    # runs a shell command and checks its status
    print(f"running: {cmd}")
    result = subprocess.run(cmd, shell=True)
    if result.returncode != 0:
        print(f"error while running: {cmd}")
        sys.exit(result.returncode)


def _python_exec():
    venv_py = Path('.venv/bin/python3')
    if venv_py.exists():
        return str(venv_py)


    return sys.executable


def _pidfile():
    return Path(tempfile.gettempdir()) / 'arbitrage_alert_bot.pid'


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


def _wait_for_exit(pid, timeout=8):
    deadline = time.time() + timeout
    while time.time() < deadline:
        if not _is_process_running(pid):
            return True
        time.sleep(0.2)
    return False


def _is_process_running(pid):
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except Exception:
        return False


def _read_int_env(name, default):
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        return int(raw)
    except (TypeError, ValueError):
        print(f"invalid value for {name}={raw!r}, fallback to {default}")
        return default


def _is_port_in_use(host, port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind((host, port))
            return False
        except OSError:
            return True


def _show_port_owners(port):
    # try to show who uses the port (if lsof exists)
    cmd = f"lsof -nP -iTCP:{port} -sTCP:LISTEN 2>/dev/null || true"
    subprocess.run(cmd, shell=True)


def main():
    # load environment only from the shared config path
    _load_env_file(ENV_FILE_PATH)


    print('=== starting arbitrage alert bot ===')

    # start databases in docker
    run_cmd('docker compose up -d')

    # apply db migrations
    python_exec = _python_exec()
    run_cmd(f'{python_exec} -m alembic upgrade head')

    # start uvicorn server in current terminal with reload for dev
    print('starting main server... (press ctrl+c to stop or use stop.py in another terminal)')

    env = os.environ.copy()
    env['PYTHONPATH'] = '.'

    host = os.environ.get("APP_HOST", "127.0.0.1")
    port = _read_int_env("APP_PORT", 8000)

    if _is_port_in_use(host, port):
        print(f'ERROR: TCP port {host}:{port} is already in use')
        _show_port_owners(port)
        print('Stop the existing process first: `python3 stop.py`')
        print(f'Or change APP_PORT in {ENV_FILE_PATH}')

        return

    # added --reload to simplify local development
    cmd = [
        python_exec,
        '-m',
        'uvicorn',
        'arbitrage_bot.main:app',
        '--reload',
        '--host',
        host,
        '--port',
        str(port),
    ]

    proc = subprocess.Popen(cmd, env=env, preexec_fn=os.setsid)
    _pidfile().write_text(str(proc.pid))

    try:
        proc.wait()
    except KeyboardInterrupt:
        # print first, then stop gracefully
        print('\n' * 2)
        print('=== stopping server safely ===')
        try:
            os.killpg(os.getpgid(proc.pid), signal.SIGTERM)
        except ProcessLookupError:
            pass

        if not _wait_for_exit(proc.pid):
            try:
                os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
            except ProcessLookupError:
                pass
        try:
            proc.wait(timeout=3)
        except Exception:
            pass
    finally:
        _pidfile().unlink(missing_ok=True)


if __name__ == '__main__':
    main()
