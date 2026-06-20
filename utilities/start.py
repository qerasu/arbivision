import os
import subprocess
import sys
import signal
import tempfile
import textwrap
from pathlib import Path
import time
import socket

try:
    from bootstrap import ensure_repo_on_path, env_file_path, repo_root
except ModuleNotFoundError:
    from utilities.bootstrap import ensure_repo_on_path, env_file_path, repo_root

ensure_repo_on_path()

from arbitrage_bot.core.env_loader import load_env_file
ENV_FILE_PATH = env_file_path()


def run_cmd(cmd):
    display_cmd = " ".join(cmd)
    print(f"running: {display_cmd}")
    result = subprocess.run(cmd, cwd=repo_root())
    if result.returncode != 0:
        print(f"error while running: {display_cmd}")
        sys.exit(result.returncode)


def _python_exec():
    venv_python = repo_root() / ".venv" / "bin" / "python3"
    if venv_python.exists():
        return str(venv_python)

    return sys.executable


def _runtime_mode():
    return os.environ.get("APP_RUNTIME_MODE", "all").strip().lower() or "all"


def _pidfile():
    return Path(tempfile.gettempdir()) / 'arbitrage_alert_bot.pid'


def _run_alembic_upgrade(python_exec):
    project_root = Path(__file__).resolve().parent.parent
    alembic_ini = project_root / 'alembic.ini'
    display_cmd = f'{python_exec} -m alembic -c {alembic_ini} upgrade head'
    inline_script = textwrap.dedent(
        f"""
        import sys
        from pathlib import Path

        repo_root = Path({str(project_root)!r}).resolve()
        sys.path = [p for p in sys.path if Path(p or '.').resolve() != repo_root]

        try:
            from alembic.config import main as alembic_main
        except ModuleNotFoundError:
            print('Alembic is not installed for this Python interpreter.')
            print(f'Install project dependencies for: {{sys.executable}}')
            print('Example: python -m pip install -r requirements.txt')
            raise SystemExit(1)

        sys.path.insert(0, str(repo_root))
        raise SystemExit(alembic_main(argv=['-c', {str(alembic_ini)!r}, 'upgrade', 'head']))
        """
    ).strip()

    cmd = [python_exec, '-c', inline_script]
    print(f"running: {display_cmd}")
    result = subprocess.run(cmd, cwd=repo_root())

    return result.returncode


def _wait_for_tcp_ready(host, port, timeout=5):
    deadline = time.time() + timeout

    while time.time() < deadline:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
            sock.settimeout(1.0)

            try:
                sock.connect((host, port))
                return True
            except OSError:
                time.sleep(0.5)

    return False


def _wait_for_postgres_ready(db_user, db_name, timeout=5):
    deadline = time.time() + timeout
    cmd = [
        'docker',
        'compose',
        'exec',
        '-T',
        'db',
        'pg_isready',
        '-U',
        db_user,
        '-d',
        db_name,
    ]

    while time.time() < deadline:
        result = subprocess.run(
            cmd,
            cwd=repo_root(),
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
        )
        if result.returncode == 0:
            return True
        time.sleep(0.5)

    return False


def _run_alembic_upgrade_with_retry(python_exec, db_host, db_port, retries=5):
    if not _wait_for_tcp_ready(db_host, db_port):
        print(f"Database is not ready on {db_host}:{db_port}")
        sys.exit(1)

    db_user = os.environ.get("POSTGRES_USER", "arb_user")
    db_name = os.environ.get("POSTGRES_DB", "arbitrage_db")
    
    if not _wait_for_postgres_ready(db_user, db_name):
        print(f"Postgres is not ready for {db_user}@{db_name}")
        sys.exit(1)

    for attempt in range(1, retries + 1):
        returncode = _run_alembic_upgrade(python_exec)
        if returncode == 0:
            return

        if attempt == retries:
            project_root = Path(__file__).resolve().parent.parent
            display_cmd = f'{python_exec} -m alembic -c {project_root / "alembic.ini"} upgrade head'
            print(f"Error while running: {display_cmd}")
            sys.exit(returncode)

        print(f"Database is starting up, retrying alembic ({attempt}/{retries})...")
        time.sleep(1.5)


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
    try:
        subprocess.run(["lsof", "-nP", f"-iTCP:{port}", "-sTCP:LISTEN"])
    except FileNotFoundError:
        pass


def main():
    # load environment only from the shared config path
    load_env_file(str(ENV_FILE_PATH))

    print('=== starting arbitrage alert bot ===')

    # start databases in docker
    run_cmd(["docker", "compose", "up", "-d"])

    # apply db migrations
    python_exec = _python_exec()
    db_host = os.environ.get("POSTGRES_HOST", "localhost")
    db_port = _read_int_env("POSTGRES_PORT", 5432)
    _run_alembic_upgrade_with_retry(python_exec, db_host, db_port)

    # start uvicorn server in current terminal with reload for dev
    print('starting main server... (press ctrl+c to stop or use utilities/stop.py in another terminal)')

    env = os.environ.copy()
    env['PYTHONPATH'] = str(repo_root())

    host = os.environ.get("APP_HOST", "127.0.0.1")
    port = _read_int_env("APP_PORT", 8000)
    runtime_mode = _runtime_mode()

    if _is_port_in_use(host, port):
        print(f'ERROR: TCP port {host}:{port} is already in use')
        _show_port_owners(port)
        print('Stop the existing process first by using: `python utilities/stop.py`')
        print(f'Or change APP_PORT in {ENV_FILE_PATH}')

        return

    print(f'runtime mode: {runtime_mode}')

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

    proc = subprocess.Popen(cmd, env=env, process_group=0, cwd=repo_root())
    _pidfile().write_text(str(proc.pid))

    try:
        proc.wait()
    except KeyboardInterrupt:
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
