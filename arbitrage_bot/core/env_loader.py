import os


def load_env_file(path):
    # preserve values explicitly supplied by the process environment
    try:
        f = open(path, "r", encoding="utf-8")
    except FileNotFoundError:
        return

    with f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue

            if "=" not in line:
                continue

            key, val = line.split("=", 1)
            key = key.strip().removeprefix("export ").strip()  # accept shell-compatible export syntax
            val = val.strip()
            if not key:
                continue

            if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
                val = val[1:-1]

            os.environ.setdefault(key, val)
