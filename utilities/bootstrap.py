import sys
from pathlib import Path


def repo_root():
    return Path(__file__).resolve().parent.parent


def env_file_path():
    return Path.home() / ".config" / "arbivision" / ".env"


def ensure_repo_on_path():
    root = repo_root()
    root_str = str(root)
    if root_str not in sys.path:
        sys.path.insert(0, root_str)
    return root