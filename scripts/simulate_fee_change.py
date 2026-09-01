"""Write a scratch copy of the extract with one fee override rescheduled and repriced.

Point DBT_DATA_DIR at the output and re-run the snapshot to see the check strategy open a
second version of that change_id rather than overwrite the first. The change is invented here
so the snapshot can be exercised. It is not an upstream change that was observed.
"""

import argparse
import shutil
import sys
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parent.parent
SOURCE_DIR = REPO / "data"
OUTPUT_DIR = REPO / "data" / "simulated"


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source-dir", type=Path, default=SOURCE_DIR)
    parser.add_argument("--output-dir", type=Path, default=OUTPUT_DIR)
    parser.add_argument(
        "--multiplier-delta",
        type=float,
        default=0.25,
        help="added to fee_multiplier on the mutated row",
    )
    parser.add_argument(
        "--reschedule-hours",
        type=int,
        default=6,
        help="added to scheduled_ts on the mutated row",
    )
    args = parser.parse_args()

    source = args.source_dir / "fee_overrides.parquet"
    if not source.exists():
        print(f"No extract at {source}. Run scripts/extract.py first.", file=sys.stderr)
        return 1

    args.output_dir.mkdir(parents=True, exist_ok=True)
    for parquet in sorted(args.source_dir.glob("*.parquet")):
        shutil.copy2(parquet, args.output_dir / parquet.name)

    con = duckdb.connect()
    target = args.output_dir / "fee_overrides.parquet"

    # Lowest change_id, so the row picked is the same on every run.
    change_id = con.execute(
        f"select min(change_id) from '{source}'"
    ).fetchone()[0]

    before = con.execute(
        f"select fee_multiplier, scheduled_ts from '{source}' where change_id = ?",
        [change_id],
    ).fetchone()

    con.execute(
        f"""
        create table mutated as
        select
            change_id, event_ticker, series_ticker, fee_type,
            case when change_id = ? then fee_multiplier + ? else fee_multiplier end
                as fee_multiplier,
            case when change_id = ? then scheduled_ts + ? else scheduled_ts end
                as scheduled_ts,
            first_seen_ms
        from '{source}'
        """,
        [change_id, args.multiplier_delta, change_id, args.reschedule_hours * 3600],
    )
    con.execute(f"copy mutated to '{target}' (format parquet)")

    after = con.execute(
        f"select fee_multiplier, scheduled_ts from '{target}' where change_id = ?",
        [change_id],
    ).fetchone()

    print(f"change_id {change_id}")
    print(f"  fee_multiplier {before[0]} -> {after[0]}")
    print(f"  scheduled_ts   {before[1]} -> {after[1]}")
    print(f"written to {args.output_dir}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
