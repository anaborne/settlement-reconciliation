"""Export the forward-test sqlite to parquet under data/.

The only file in this repository that reads the private source. Every column that leaves
sqlite is named here, so the export surface is reviewable in one place before data lands in
a repository that may go public. Columns no model reads are not exported.

Epoch columns keep their source units and suffixes (_ms milliseconds, _ts seconds). Staging
converts them to timestamps.
"""

import argparse
import os
import re
import sqlite3
import sys
from pathlib import Path

import duckdb

DEFAULT_SOURCE = Path.home() / "Dev" / "kalshi-bot" / "evidence" / "data" / "forward_test.sqlite"
OUTPUT_DIR = Path(__file__).resolve().parent.parent / "data"

RULE_VERSION = re.compile(r"^[a-z0-9-]+/(\d+)$")


def to_ruleset(value):
    """Strip the private strategy name, keep the version ordinal.

    The ordinal is checkable against this repository's own data. The name it was attached to
    is not, and it describes a system that is not published.
    """
    match = RULE_VERSION.match(value)
    if match is None:
        raise ValueError(f"rule_version {value!r} does not parse as <name>/<ordinal>")
    return f"ruleset-{match.group(1)}"


def strip_rule_version(index):
    def transform(row):
        row = list(row)
        row[index] = to_ruleset(row[index])
        return tuple(row)

    return transform


EXPORTS = {
    "runs": {
        "columns": [
            ("run_id", "INTEGER"),
            ("started_at_ms", "BIGINT"),
            ("finished_at_ms", "BIGINT"),
            ("ruleset_version", "VARCHAR"),
        ],
        "query": "SELECT id, started_at_ms, finished_at_ms, rule_version FROM runs",
        "transform": strip_rule_version(3),
    },
    "decisions": {
        "columns": [
            ("ticker", "VARCHAR"),
            ("run_id", "INTEGER"),
            ("series_ticker", "VARCHAR"),
            ("event_ticker", "VARCHAR"),
            ("decided_at_ms", "BIGINT"),
            ("occurrence_ts", "BIGINT"),
            ("yes_bid_cents", "INTEGER"),
            ("yes_ask_cents", "INTEGER"),
            ("mid_cents", "DOUBLE"),
            ("spread_cents", "INTEGER"),
            ("fee_rate", "DOUBLE"),
            ("ruleset_version", "VARCHAR"),
        ],
        "query": """
            SELECT ticker, run_id, series_ticker, event_ticker, decided_at_ms, occurrence_ts,
                   yes_bid_cents, yes_ask_cents, mid_cents, spread_cents, fee_rate, rule_version
            FROM decisions
        """,
        "transform": strip_rule_version(11),
    },
    "settlements": {
        "columns": [
            ("ticker", "VARCHAR"),
            ("result", "VARCHAR"),
            ("close_ts", "BIGINT"),
            ("net_cents", "DOUBLE"),
            ("joined_at_ms", "BIGINT"),
        ],
        "query": "SELECT ticker, result, close_ts, net_cents, joined_at_ms FROM settlements",
    },
    "fee_overrides": {
        "columns": [
            ("change_id", "VARCHAR"),
            ("event_ticker", "VARCHAR"),
            ("series_ticker", "VARCHAR"),
            ("fee_type", "VARCHAR"),
            ("fee_multiplier", "DOUBLE"),
            ("scheduled_ts", "BIGINT"),
            ("first_seen_ms", "BIGINT"),
        ],
        "query": """
            SELECT change_id, event_ticker, series_ticker, fee_type, fee_multiplier,
                   scheduled_ts, first_seen_ms
            FROM fee_overrides
        """,
    },
    "unscoreable": {
        "columns": [
            ("ticker", "VARCHAR"),
            ("result", "VARCHAR"),
            ("status", "VARCHAR"),
        ],
        "query": "SELECT ticker, result, status FROM unscoreable",
    },
}


def print_contract():
    for table, spec in EXPORTS.items():
        names = ", ".join(f"{name} {type_}" for name, type_ in spec["columns"])
        print(f"{table}: {names}")


def export(source, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)
    sqlite = sqlite3.connect(f"file:{source}?mode=ro", uri=True)
    duck = duckdb.connect()
    try:
        for table, spec in EXPORTS.items():
            rows = sqlite.execute(spec["query"]).fetchall()
            transform = spec.get("transform")
            if transform is not None:
                rows = [transform(row) for row in rows]

            ddl = ", ".join(f"{name} {type_}" for name, type_ in spec["columns"])
            placeholders = ", ".join("?" for _ in spec["columns"])
            duck.execute(f"CREATE OR REPLACE TABLE staged ({ddl})")
            duck.executemany(f"INSERT INTO staged VALUES ({placeholders})", rows)

            target = output_dir / f"{table}.parquet"
            duck.execute(f"COPY staged TO '{target}' (FORMAT PARQUET)")
            print(f"{table}: {len(rows)} rows -> {target}")
    finally:
        sqlite.close()
        duck.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--source",
        type=Path,
        default=Path(os.environ.get("FORWARD_TEST_SQLITE", DEFAULT_SOURCE)),
    )
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="print the export contract and write nothing",
    )
    args = parser.parse_args()

    if args.dry_run:
        print_contract()
        return 0

    if not args.source.exists():
        print(
            f"Source sqlite not found at {args.source}.\n"
            "This file comes from a private forward test and is not distributed with this "
            "repository. Set FORWARD_TEST_SQLITE or pass --source to point at it. The "
            "committed fixture under fixtures/ runs the models without it.",
            file=sys.stderr,
        )
        return 1

    export(args.source, args.output_dir)
    return 0


if __name__ == "__main__":
    sys.exit(main())
