"""Mutation harness. Break the data on purpose and check the right test notices.

A test that returns zero rows because its SQL is wrong looks exactly like a test that returns
zero rows because the data is clean. The only way to tell them apart is to introduce a fault and
watch the test fail. Each case below names a control, injects one violation of it, and asserts
that dbt reports that specific node as failed.

Cases marked unfalsifiable are the point of the exercise rather than an omission. They record a
control that cannot fail by construction, so nobody has to rediscover that later.
"""

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

import duckdb

REPO = Path(__file__).resolve().parent.parent
FIXTURES = REPO / "fixtures"

DATA_CASES = [
    {
        "control": "unique_stg_decisions_ticker",
        "fault": "the same ticker decided twice",
        "table": "decisions",
        "sql": "select * from '{src}' union all (select * from '{src}' order by ticker limit 1)",
        "row_delta": 1,
    },
    {
        "control": "unique_stg_settlements_ticker",
        "fault": "the settlement feed emits a ticker twice",
        "table": "settlements",
        "sql": "select * from '{src}' union all (select * from '{src}' order by ticker limit 1)",
        "row_delta": 1,
    },
    {
        "control": "not_null_stg_decisions_occurrence_at",
        "fault": "a decision with no event time",
        "table": "decisions",
        "sql": """
            select * replace (
                case when ticker = (select min(ticker) from '{src}') then null
                     else occurrence_ts end as occurrence_ts
            ) from '{src}'
        """,
        "row_delta": 0,
    },
    {
        "control": "accepted_values_stg_settlements_result__yes__no",
        "fault": "an outcome class the models do not know",
        "table": "settlements",
        "sql": """
            select * replace (
                case when ticker = (select min(ticker) from '{src}') then 'void'
                     else result end as result
            ) from '{src}'
        """,
        "row_delta": 0,
    },
    {
        "control": "accepted_values_stg_fee_overrides_fee_type__quadratic__quadratic_with_maker_fees",
        "fault": "an unpriceable fee formula published upstream",
        "table": "fee_overrides",
        "sql": """
            select * replace (
                case when change_id = (select min(change_id) from '{src}') then 'linear'
                     else fee_type end as fee_type
            ) from '{src}'
        """,
        "row_delta": 0,
    },
    {
        "control": "dbt_utils_accepted_range_stg_decisions_mid_cents__True__99__1",
        "fault": "a quote outside the 1 to 99 contract bounds",
        "table": "decisions",
        "sql": """
            select * replace (
                case when ticker = (select min(ticker) from '{src}') then 150.0
                     else mid_cents end as mid_cents
            ) from '{src}'
        """,
        "row_delta": 0,
    },
    {
        "control": "dbt_utils_accepted_range_stg_decisions_fee_rate__True__1__0",
        "fault": "a fee rate expressed as a percentage rather than a fraction",
        "table": "decisions",
        "sql": """
            select * replace (
                case when ticker = (select min(ticker) from '{src}') then 7.0
                     else fee_rate end as fee_rate
            ) from '{src}'
        """,
        "row_delta": 0,
    },
    {
        "control": "relationships_stg_decisions_run_id__run_id__ref_stg_runs_",
        "fault": "a decision attributed to a run the log has never heard of",
        "table": "decisions",
        "sql": """
            select * replace (
                case when ticker = (select min(ticker) from '{src}') then 999
                     else run_id end as run_id
            ) from '{src}'
        """,
        "row_delta": 0,
    },
    {
        "control": "dbt_utils_equal_rowcount_int_decision_settlement_ref_stg_decisions_",
        "fault": "a duplicated settlement fanning the join out past one row per decision",
        "table": "settlements",
        "sql": "select * from '{src}' union all (select * from '{src}' order by ticker limit 1)",
        "row_delta": 1,
    },
    {
        "control": "assert_no_settlement_without_decision",
        "fault": "the feed settles a market the ledger never traded",
        "table": "settlements",
        "sql": """
            select * from '{src}'
            union all
            (select 'GHOST-MARKET-1', 'yes', close_ts, net_cents, joined_at_ms
             from '{src}' order by ticker limit 1)
        """,
        "row_delta": 1,
    },
    {
        "control": "assert_decisions_inside_run_window",
        "fault": "a decision timestamped outside the run it claims",
        "table": "decisions",
        "sql": """
            select * replace (
                case when ticker = (select min(ticker) from '{src}') then 1
                     else decided_at_ms end as decided_at_ms
            ) from '{src}'
        """,
        "row_delta": 0,
    },
    {
        "control": "assert_no_unexplained_breaks",
        "fault": "a twelfth unexplained break, one past the accepted backlog",
        "table": "decisions",
        "sql": """
            select * from '{src}'
            union all
            (select 'NEW-BREAK-1', run_id, series_ticker, event_ticker, decided_at_ms,
                    1787545800, yes_bid_cents, yes_ask_cents, mid_cents, spread_cents,
                    fee_rate, ruleset_version
             from '{src}' order by ticker limit 1)
        """,
        "row_delta": 1,
    },
    {
        "control": "assert_daily_match_rate_above_floor",
        "fault": "a day where the feed settles almost nothing",
        "table": "settlements",
        "sql": """
            select * from '{src}'
            where ticker not in (
                select ticker from '{decisions}'
                where cast(epoch_ms(decided_at_ms) as date) = date '2026-08-25'
            )
        """,
        "row_delta": -42,
    },
]

FILE_CASES = [
    {
        "control": "assert_unsettled_decisions_are_classified",
        "fault": "a reason-code branch that drops a row instead of labelling it",
        "path": "models/marts/fct_reporting_breaks.sql",
        "find": "    where not joined.is_settled",
        "replace": "    where not joined.is_settled and joined.series_ticker != 'KXITFMATCH'",
    },
    {
        "control": "fct_reporting_breaks",
        "fault": "a mart column whose type drifts from its declared contract",
        "path": "models/marts/_marts.yml",
        "find": "        data_type: bigint",
        "replace": "        data_type: varchar",
    },
]

# Controls that cannot be made to fail. is_settled is defined as "the settlement join matched",
# so every row the relationships test selects has a parent by construction. The test documents
# intent; equal_rowcount is what actually guards the join.
UNFALSIFIABLE = [
    {
        "control": "relationships_int_decision_settlement_ticker__ticker__ref_stg_settlements_",
        "fault": "drop a settlement a decision points at",
        "reason": "is_settled is derived from the same join the test checks",
        "table": "settlements",
        "sql": "select * from '{src}' where ticker != (select min(ticker) from '{src}')",
        "row_delta": -1,
    },
]


def dbt(project_dir, data_dir, *args):
    env = dict(os.environ, DBT_DATA_DIR=str(data_dir), DBT_PROFILES_DIR=str(REPO))
    subprocess.run(
        [str(REPO / ".venv" / "bin" / "dbt"), *args, "--no-partial-parse",
         "--target-path", str(data_dir / "target")],
        capture_output=True, text=True, env=env, cwd=str(project_dir),
    )
    results = data_dir / "target" / "run_results.json"
    manifest = data_dir / "target" / "manifest.json"
    if not results.exists() or not manifest.exists():
        return {}
    names = {uid: n["name"] for uid, n in json.loads(manifest.read_text())["nodes"].items()}
    return {names.get(r["unique_id"], r["unique_id"]): r["status"]
            for r in json.loads(results.read_text())["results"]}


def run_case(case, workdir):
    project = workdir / "project"
    data = workdir / "data"
    for path in (project, data):
        if path.exists():
            shutil.rmtree(path)
    shutil.copytree(REPO, project, ignore=shutil.ignore_patterns(
        ".venv", "target", "logs", ".git", "data", "__pycache__", "*.duckdb*"))
    shutil.copytree(FIXTURES, data)

    if "table" in case:
        con = duckdb.connect()
        src = data / f"{case['table']}.parquet"
        sql = case["sql"].format(src=src, decisions=data / "decisions.parquet")
        before = con.execute(f"select count(*) from '{src}'").fetchone()[0]
        con.execute(f"create table mutated as {sql}")
        after = con.execute("select count(*) from mutated").fetchone()[0]
        if "row_delta" in case and after - before != case["row_delta"]:
            con.close()
            return "BAD FAULT", f"row delta {after - before}, expected {case['row_delta']}"
        con.execute(f"copy mutated to '{src}' (format parquet)")
        con.close()
    else:
        target = project / case["path"]
        text = target.read_text()
        if case["find"] not in text:
            return "MISSING", f"pattern not found in {case['path']}"
        target.write_text(text.replace(case["find"], case["replace"], 1))

    # Models first, then only the named control. dbt build skips a node when any upstream test
    # fails, which would let one control mask another and report a false miss.
    status = dbt(project, data, "run").get(case["control"])
    if status in ("fail", "error"):
        return "CAUGHT", status

    status = dbt(project, data, "test", "--select", case["control"]).get(case["control"])
    if status in ("fail", "error"):
        return "CAUGHT", status
    if status is None:
        return "NOT RUN", "control did not execute"
    return "MISSED", status


def main():
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--keep", action="store_true", help="leave the scratch directory behind")
    args = parser.parse_args()

    if not FIXTURES.exists():
        print(f"No fixture at {FIXTURES}. Run scripts/build_fixture.py first.", file=sys.stderr)
        return 1

    workdir = Path(tempfile.mkdtemp(prefix="verify-controls-"))
    failures = 0
    try:
        print(f"{'result':8s}  {'control':64s}  fault")
        print("-" * 120)
        for case in DATA_CASES + FILE_CASES:
            outcome, detail = run_case(case, workdir)
            if outcome != "CAUGHT":
                failures += 1
                detail = f"{case['fault']}  [{outcome}: {detail}]"
            else:
                detail = case["fault"]
            print(f"{outcome:8s}  {case['control']:64s}  {detail}")
        for case in UNFALSIFIABLE:
            outcome, detail = run_case(case, workdir)
            if outcome == "CAUGHT":
                failures += 1
                print(f"{'SURPRISE':8s}  {case['control']:64s}  "
                      f"caught {case['fault']}, so it is falsifiable after all")
            else:
                print(f"{'INERT':8s}  {case['control']:64s}  {case['reason']}")
    finally:
        if not args.keep:
            shutil.rmtree(workdir, ignore_errors=True)
        else:
            print(f"\nscratch kept at {workdir}")

    print()
    total = len(DATA_CASES) + len(FILE_CASES)
    print(f"{total - failures} of {total} injected faults caught by the control that names them")
    for case in UNFALSIFIABLE:
        print(f"{case['control']} cannot fail: {case['reason']}")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
