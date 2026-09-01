"""Write the committed fixture under fixtures/ from the extract in data/.

CI runs dbt build against the fixture so a checkout without the private sqlite still exercises
every model and test.

Only fee_overrides is reduced. The reconciliation tables are copied whole because the reason
codes and the daily match rates are properties of the whole decision population: the settlement
window comes from the largest close-to-load lag across settled rows, and a daily match rate is
a ratio over a full day. Subsampling either would move the numbers the tests exist to protect.
"""

import argparse
import shutil
import sys
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parent.parent
COPIED_WHOLE = ["runs", "decisions", "settlements", "unscoreable"]


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--source-dir", type=Path, default=REPO / "data")
    parser.add_argument("--output-dir", type=Path, default=REPO / "fixtures")
    parser.add_argument(
        "--fee-overrides-per-type",
        type=int,
        default=25,
        help="rows kept per fee_type, lowest change_id first",
    )
    args = parser.parse_args()

    missing = [t for t in COPIED_WHOLE if not (args.source_dir / f"{t}.parquet").exists()]
    if missing:
        print(
            f"No extract at {args.source_dir} for {', '.join(missing)}. "
            "Run scripts/extract.py first.",
            file=sys.stderr,
        )
        return 1

    args.output_dir.mkdir(parents=True, exist_ok=True)
    for table in COPIED_WHOLE:
        name = f"{table}.parquet"
        shutil.copy2(args.source_dir / name, args.output_dir / name)
        print(f"{table}: copied whole")

    source = args.source_dir / "fee_overrides.parquet"
    target = args.output_dir / "fee_overrides.parquet"
    con = duckdb.connect()
    con.execute(
        f"""
        create table sampled as
        select * exclude (rank) from (
            select *, row_number() over (partition by fee_type order by change_id) as rank
            from '{source}'
        )
        where rank <= {args.fee_overrides_per_type}
        """
    )
    con.execute(f"copy sampled to '{target}' (format parquet)")
    kept = con.execute(f"select count(*) from '{target}'").fetchone()[0]
    total = con.execute(f"select count(*) from '{source}'").fetchone()[0]
    print(f"fee_overrides: {kept} of {total} rows, {args.fee_overrides_per_type} per fee_type")
    return 0


if __name__ == "__main__":
    sys.exit(main())
