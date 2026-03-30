import os


def load_env_file(path):
    # loads key=value pairs from a .env file without overriding existing env vars
    if not os.path.exists(path):
        raise ValueError("path provided to the .env file does not exist")

    with open(path, "r", encoding="utf-8") as f:
        for raw_line in f:
            line = raw_line.strip()
            if not line or line.startswith("#"):
                continue

            if "=" not in line:
                continue

            key, val = line.split("=", 1)
            key = key.strip().removeprefix("export ").strip() # for bash scripts
            val = val.strip()
            if not key:
                continue

            if (val.startswith('"') and val.endswith('"')) or (val.startswith("'") and val.endswith("'")):
                val = val[1:-1]

            os.environ.setdefault(key, val)