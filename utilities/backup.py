import argparse
import os
from datetime import datetime
from pathlib import Path
import subprocess

try:
    from bootstrap import ensure_repo_on_path, env_file_path
except ModuleNotFoundError:
    from utilities.bootstrap import ensure_repo_on_path, env_file_path

ensure_repo_on_path()

from arbitrage_bot.core.env_loader import load_env_file

ENV_FILE_PATH = env_file_path()


def _repo_root():
    return Path(__file__).resolve().parent.parent


def _parse_args():
    parser = argparse.ArgumentParser(
        description="Create a PostgreSQL backup from the running docker compose project."
    )
    parser.add_argument(
        "--output-dir",
        default="backups",
        help="Subdirectory inside the repository root where backups will be stored.",
    )
    return parser.parse_args()


def _load_env_if_exists():
    if ENV_FILE_PATH.exists():
        load_env_file(str(ENV_FILE_PATH))


def _is_within_repo(path):
    repo_root = _repo_root().resolve()
    resolved = path.resolve()
    return resolved == repo_root or repo_root in resolved.parents


def _resolve_output_dir(raw_output_dir):
    output_dir = (_repo_root() / raw_output_dir).resolve()

    if not _is_within_repo(output_dir):
        print("error: output directory must stay inside the repository root")
        raise SystemExit(1)

    return output_dir


def _backup_filename():
    database_name = os.environ.get("POSTGRES_DB", "arbitrage_db").strip() or "arbitrage_db"
    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S_%f")
    return f"{database_name}_{timestamp}.dump"


def _run_backup(target_path):
    partial_path = target_path.with_name(f"{target_path.name}.partial")
    cmd = [
        "docker",
        "compose",
        "exec",
        "-T",
        "db",
        "sh",
        "-lc",
        'PGPASSWORD="$POSTGRES_PASSWORD" exec pg_dump -U "$POSTGRES_USER" -d "$POSTGRES_DB" -Fc',
    ]

    if target_path.exists():
        print(f"error: target file already exists: {target_path}")
        raise SystemExit(1)

    if partial_path.exists():
        print(f"error: partial backup file already exists: {partial_path}")
        raise SystemExit(1)

    try:
        with partial_path.open("wb") as output_file:
            result = subprocess.run(
                cmd,
                cwd=_repo_root(),
                stdout=output_file,
                stderr=subprocess.PIPE,
            )
    except FileNotFoundError as exc:
        print(f"error: failed to run backup command: {exc}")
        raise SystemExit(1) from exc

    if result.returncode != 0:
        print("error: backup failed")
        stderr = result.stderr.decode("utf-8", errors="replace").strip()
        if stderr:
            print(stderr)
        print(f"partial file was left for inspection: {partial_path}")
        raise SystemExit(result.returncode or 1)

    partial_path.rename(target_path)


def main():
    args = _parse_args()
    _load_env_if_exists()

    output_dir = _resolve_output_dir(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    target_path = output_dir / _backup_filename()

    print("=== creating postgres backup ===")
    print(f"output directory: {output_dir}")
    print(f"backup file: {target_path.name}")

    _run_backup(target_path)

    size_bytes = target_path.stat().st_size
    print("backup created successfully")
    print(f"path: {target_path}")
    print(f"size: {size_bytes} bytes")


if __name__ == "__main__":
    main()