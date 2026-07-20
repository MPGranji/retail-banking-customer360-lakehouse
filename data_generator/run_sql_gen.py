"""
Run SQL-mode data generation for both databases.

Usage:
    python run_sql_gen.py [--config config.yaml] [--oracle-only] [--postgres-only]
"""
import argparse
import sys
import time
from datetime import date
from pathlib import Path

import yaml

sys.path.insert(0, str(Path(__file__).parent))

from loaders import sql_runner


def parse_cob(cfg: dict) -> date:
    raw = cfg.get("cob_dt", "2024-12-30")
    if isinstance(raw, date):
        return raw
    return date.fromisoformat(str(raw))


def load_config(path: str) -> dict:
    full = Path(__file__).parent / path
    if not full.exists():
        sys.exit(f"Config file not found: {full}")
    with open(full, encoding="utf-8") as f:
        return yaml.safe_load(f)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run oracle_gen.sql & postgres_gen.sql")
    parser.add_argument("--config", default="config.yaml", help="Path to config YAML (default: config.yaml)")
    parser.add_argument("--oracle-only",   action="store_true", help="Only run oracle_gen.sql")
    parser.add_argument("--postgres-only", action="store_true", help="Only run postgres_gen.sql")
    args = parser.parse_args()

    cfg    = load_config(args.config)
    cob    = parse_cob(cfg)
    params = sql_runner.build_params(cfg, cob)

    run_oracle   = not args.postgres_only
    run_postgres = not args.oracle_only

    t0 = time.time()
    print(f"cob_dt={cob}  customers={params['n_customers']:,}  seed={params['seed']}\n")

    if run_oracle:
        print("[1/2] Oracle — core_banking (oracle_gen.sql)...")
        sql_runner.run_oracle(cfg, params)
        print("      Done.\n")

    if run_postgres:
        print("[2/2] PostgreSQL — card_crm (postgres_gen.sql)...")
        sql_runner.run_postgres(cfg, params)
        print("      Done.\n")

    print(f"Finished in {time.time() - t0:.1f}s")


if __name__ == "__main__":
    main()
