#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from src.polybot.copybot_dashboard import DashboardServer
from src.polybot.copybot_models import RuntimeConfig
from src.polybot.copybot_runtime import CopyTradingRuntime
from src.polybot.copybot_storage import build_repository


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Polymarket copy trading bot")
    sub = parser.add_subparsers(dest="command", required=True)

    run_cmd = sub.add_parser("run", help="Run the copy trading runtime")
    run_cmd.add_argument("--config", required=True, help="Path to JSON config")
    run_cmd.add_argument("--max-cycles", type=int, default=None, help="Optional number of polling cycles for dry runs")

    dash_cmd = sub.add_parser("serve-dashboard", help="Serve dashboard from persisted storage")
    dash_cmd.add_argument("--config", required=True, help="Path to JSON config")

    check_cmd = sub.add_parser("validate-config", help="Validate config and print summary")
    check_cmd.add_argument("--config", required=True, help="Path to JSON config")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    config = RuntimeConfig.from_file(Path(args.config))

    if args.command == "validate-config":
        print(f"mode={config.wallet.mode}")
        print(f"traders={len(config.traders)}")
        print(f"mongo_enabled={config.mongo.enabled}")
        print(f"dashboard={config.dashboard.host}:{config.dashboard.port}")
        return

    if args.command == "serve-dashboard":
        repository = build_repository(config)
        server = DashboardServer(
            repository=repository,
            host=config.dashboard.host,
            port=config.dashboard.port,
            refresh_sec=config.dashboard.refresh_sec,
        )
        print(f"Dashboard listening on http://{config.dashboard.host}:{config.dashboard.port}")
        server.serve_forever()
        return

    runtime = CopyTradingRuntime(config)
    runtime.run(max_cycles=args.max_cycles)


if __name__ == "__main__":
    main()
