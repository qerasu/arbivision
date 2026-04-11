import argparse
import json
import os
import sys
from pathlib import Path
from urllib import error
from urllib import request

from arbitrage_bot.core.env_loader import load_env_file

ENV_FILE_PATH = Path.home() / ".config" / "arbivision" / ".env"


def _parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--host", default=os.environ.get("APP_HOST", "127.0.0.1"))
    parser.add_argument("--port", type=int, default=int(os.environ.get("APP_PORT", "8000")))
    parser.add_argument("--scheme", default="http")
    parser.add_argument("--verbose", action="store_true")
    return parser.parse_args()


def _base_url(args):
    return f"{args.scheme}://{args.host}:{args.port}"


def _request_json(url, headers=None):
    req = request.Request(url, headers=headers or {})
    with request.urlopen(req, timeout=10) as response:
        payload = response.read().decode("utf-8")
        return response.status, json.loads(payload)


def _print_json(title, payload):
    print(f"\n=== {title} ===")
    print(json.dumps(payload, ensure_ascii=False, indent=2))


def _print_summary(name, payload):
    if name == "health":
        print(f"health: {payload.get('status')}")
        return

    if name == "status":
        markets = payload.get("market_counts", {})
        pairs = payload.get("pair_counts", {})
        opportunities = payload.get("opportunity_counts", {})
        alerts = payload.get("alert_counts", {})
        print(
            "status:"
            f" markets={markets.get('total', 0)}"
            f" pairs={pairs.get('total', 0)}"
            f" approved={pairs.get('approved', 0)}"
            f" opportunities={opportunities.get('total', 0)}"
            f" queued_fanout={opportunities.get('queued_fanout', 0)}"
            f" queued_alerts={alerts.get('queued', 0)}"
        )


def _run_check(name, url, headers=None, verbose=False):
    try:
        status_code, payload = _request_json(url, headers=headers)
    except error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        print(f"failed: HTTP {exc.code}")
        print(body)
        raise SystemExit(1)
    except error.URLError as exc:
        print(f"failed: {exc.reason}")
        raise SystemExit(1)

    print(f"{name}: HTTP {status_code}")
    if verbose:
        _print_json(name, payload)
    else:
        _print_summary(name, payload)
    return payload


def main():
    load_env_file(ENV_FILE_PATH)
    args = _parse_args()
    base_url = _base_url(args)

    _run_check("health", f"{base_url}/api/health", verbose=args.verbose)
    _run_check("status", f"{base_url}/api/status", verbose=args.verbose)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\ninterrupted by user")
        sys.exit(130)