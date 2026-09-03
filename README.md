# Reconciling a Trade Decision Ledger Against an Exchange Settlement Feed

dbt models that join a trade decision ledger to the exchange settlement feed that should
explain it, classify every decision the feed has not settled, and fail the build when the
unexplained population grows past the size recorded here.

The input is a forward test of a Kalshi trading rule run between 2026-08-24 and 2026-08-29:
281 decisions from 5 of the 12 collection runs in the run log, against a settlement feed
carrying 263 rows. 263 of the 281 decisions settle, a match rate of 93.59%. The 18 that do not
are the subject of the reconciliation.

Model and column documentation with the lineage graph:
[anaborne.github.io/settlement-reconciliation](https://anaborne.github.io/settlement-reconciliation/),
published from `dbt docs generate` on every push to `main`.

## What it does

1. `scripts/extract.py` reads the forward-test sqlite and writes five parquet files into
   `data/`. It is the only file here that touches the private source, and it names every
   exported column so the export surface is reviewable in one place.
2. Five staging models type the parquet, convert epoch columns to timestamps, and rename to
   business names. No classification happens in staging.
3. `int_decision_settlement` left joins decisions to settlements, one row per decision, with
   settlement columns null where the feed produced nothing. The count of unsettled rows here
   is the raw size of the gap, before any judgment about what caused it.
4. `fct_reporting_breaks` carries one row per unsettled decision with a `reason_code` and a
   `severity`.
5. `fct_daily_reconciliation` reduces that to one row per decision date: decisions taken,
   settlements matched, match rate, break count split by reason code, net settled result.
6. A snapshot versions the upstream fee schedule, so a repriced or rescheduled fee change opens
   a new version instead of overwriting the old one.
7. Both fact tables carry an enforced dbt contract, so a column that changes name, type, or
   position fails the build instead of shipping.
8. 82 data tests run on `dbt build`, 77 generic and 5 singular, covering grain, join keys,
   referential integrity between matched decisions and settlements, the enumerated values of
   `result`, `fee_type`, `reason_code`, and `severity`, the ranges of `fee_rate`, `mid_cents`,
   and `match_rate`, the size of the unexplained population, the daily match-rate floor,
   agreement between the ledger and the run log, and both directions of the reconciliation.

## Methodology

### The settlement window

The settlement feed publishes no load-completion marker, so how far it has run has to be
inferred from the rows it produced. Two figures come out of the 263 settled rows:

- Watermark, the newest `loaded_at` in the feed: 2026-08-29 20:53:54 UTC.
- Close-to-load lag, `loaded_at - closed_at`: minimum 87 s, maximum 72828 s (20.2 h), n=263.

The settlement window opens at `watermark - 72828 s` and has no upper bound, since an event
that has not happened yet cannot have settled. A decision whose event falls inside the window
has not had longer to settle than the feed has ever taken. A decision whose event falls before
the window has, so the absence of a settlement row for it needs an explanation.

Both figures are computed in `fct_reporting_breaks` from the settled rows, so the window moves
with the data rather than sitting in a constant.

### Reason codes

Codes are assigned in precedence order. The first rule that matches wins.

| `reason_code` | `severity` | Rule | n |
|---|---|---|---|
| `unscoreable_market` | `low` | Present in the `unscoreable` feed, which lists markets the exchange finalised without a binary result | 1 |
| `settlement_pending` | `info` | Event occurs inside the settlement window defined above | 6 |
| `unexplained` | `high` | Everything else | 11 |

`unscoreable_market` is checked first because a market the feed has finalised is settled as far
as the feed is concerned, whatever the timing says. The one row carrying this code occurs
63294 s inside the settlement window, so timing alone would have called it pending. The feed
reports it as a scalar market with status `finalized`, and a finalised row is not pending.

`settlement_pending` is not a break. All 6 rows come from the last collection day, and their events
occur 366 s to 7566 s after the watermark, so the feed had not reached them when it last ran. They are reported so a morning read shows the whole gap, and they carry
`info` so they do not fail a build.

`unexplained` is 11 rows. Each occurs between 380406 s and 418206 s (4.4 to 4.8 days) before
the settlement window opens, over five times the largest close-to-load lag the feed has shown.
Nothing in the extract accounts for them.

### What was ruled out for the 11

- `resolutions`, the ledger of candidates the rule rejected, shares 0 tickers with `decisions`
  across its 2294 rows. It cannot explain a decision that was taken.
- `watchlist` covers 6 of the 18 unsettled tickers, all with status `decided`, which repeats
  what `decisions` already records.
- Series is not the discriminator. Every series carrying an unexplained row settles elsewhere in
  the extract: `KXITFMATCH` 33 of 37, `KXT20MATCH` 8 of 12, `KXATPCHALLENGERMATCH` 16 of 18,
  `KXCS2GAME` 16 of 17, `KXITFWMATCH` 17 of 18. Within the run that produced 8 of the 11,
  `KXATPCHALLENGERMATCH` carries both settled and unsettled rows, 6 and 2.
- A load outage is not the discriminator either. Ordering that run's 18 decisions by event
  time interleaves settled and unsettled rows rather than splitting them at a boundary.

The first two bullets read `resolutions` and `watchlist`. No model here uses either table and
`scripts/extract.py` does not export them, so those two are checkable only against the private
sqlite. The series and load-outage bullets are checkable from `fixtures/`.

One correlation does hold, and it is confounded. All 11 fall in the first 21 decisions, taken
between 2026-08-24 02:38 and 13:00 UTC under rulesets 1 through 3. The other 260 decisions,
taken under ruleset 5, carry 0 unexplained rows. Ruleset and collection time move together
across these 21 rows, so this data cannot separate them, and neither is offered here as a
cause.

### Reporting grain

Daily reporting keys on the UTC date of `decided_at`, not on the run start date. Runs 11 and 12
each span multiple days, so run start date puts all 281 decisions into 2 buckets. Decision date
gives 6, and the match rate varies across them:

| Decision date | Decisions | Settled | Match rate |
|---|---|---|---|
| 2026-08-24 | 40 | 29 | 72.5% |
| 2026-08-25 | 42 | 42 | 100% |
| 2026-08-26 | 44 | 44 | 100% |
| 2026-08-27 | 48 | 48 | 100% |
| 2026-08-28 | 44 | 44 | 100% |
| 2026-08-29 | 63 | 56 | 88.9% |

### What leaves the private source

`scripts/extract.py` exports 5 tables and 31 columns. Volume, depth, order-book ladders, market
titles, and the per-run rule parameters are all present in the sqlite and none of them are
exported, because no model here reads them.

One column is rewritten rather than dropped. The source `rule_version` is
`<strategy-name>/<ordinal>`, and the strategy name belongs to a system that is not published,
so it cannot be checked against anything outside the private repository. The extract keeps the
ordinal and discards the name, giving `ruleset-1` through `ruleset-5`. The ordinal is checkable
against this repository's own data: `stg_runs` shows 5 rulesets across 12 runs, and `ruleset-4`
appears in `stg_runs` with 0 decisions, as do six other runs.

`mid_cents` and `net_cents` are stored as doubles rather than integers. `mid_cents` is the
midpoint of two integer quotes and lands on half cents. `net_cents` is post-fee and is
fractional for the same reason. Every other cents column is an integer.

### Daily reporting

`fct_daily_reconciliation` is the table a human reads each morning. One row per decision date,
carrying decisions taken, settlements matched, match rate, the break population split by reason
code, and the net settled result in cents.

| Decision date | Decisions | Matched | Match rate | Runs | Rulesets | Pending | Unscoreable | Unexplained | Net cents |
|---|---|---|---|---|---|---|---|---|---|
| 2026-08-24 | 40 | 29 | 0.7250 | 4 | ruleset-1, ruleset-2, ruleset-3, ruleset-5 | 0 | 0 | 11 | -326.44 |
| 2026-08-25 | 42 | 42 | 1.0000 | 1 | ruleset-5 | 0 | 0 | 0 | 105.78 |
| 2026-08-26 | 44 | 44 | 1.0000 | 2 | ruleset-5 | 0 | 0 | 0 | 110.96 |
| 2026-08-27 | 48 | 48 | 1.0000 | 1 | ruleset-5 | 0 | 0 | 0 | -72.68 |
| 2026-08-28 | 44 | 44 | 1.0000 | 1 | ruleset-5 | 0 | 0 | 0 | -258.95 |
| 2026-08-29 | 63 | 56 | 0.8889 | 1 | ruleset-5 | 6 | 1 | 0 | 33.54 |

The run and ruleset columns put the anomalous day next to what made it unusual. 2026-08-24 is
the only date with more than one ruleset in force, at four runs and four rulesets, and it is the
only date carrying unexplained breaks. The other five dates ran one ruleset and cleared.

Break counts key on the date of the decision, not the date the settlement feed ran, so a break
stays on the day the exposure was taken.

The reason codes are pivoted into named columns rather than left long. Adding a code therefore
requires a change to this model, which keeps the taxonomy and the daily report from drifting
apart silently.

### Tests

77 generic tests cover grain, join keys, referential integrity, enumerated values, and ranges.
They are declared in the `_staging.yml`, `_intermediate.yml`, and `_marts.yml` property files
beside each model. Five singular tests carry the reconciliation logic.

`assert_no_unexplained_breaks` selects every row with `reason_code = 'unexplained'`. It warns
above 0 and errors above `unexplained_break_baseline`, a var set to 11, which is the size of
the population documented above. The build reports the backlog on every run and fails on the
twelfth row.

That design has a cost. A zero-tolerance version of this test fails today and keeps failing
until the 11 are diagnosed, so the build goes red and stays red. The baseline keeps a red build
meaningful, at the price of only catching a new break when the count rises. A break that appears
while another is resolved in the same run nets out and passes. Lowering the baseline as breaks
are cleared is manual.

`assert_daily_match_rate_above_floor` fails on any decision date whose match rate falls below
`min_daily_match_rate`, a var set to 0.70. The observed minimum is 0.7250 on 2026-08-24 (n=40),
the day carrying the whole unexplained backlog, and four of six days are at 1.0000. The floor
sits 2.5 points under the worst observed day. Any threshold above 0.7250 fails on 2026-08-24
and any threshold above 0.8889 also fails on 2026-08-29, so the floor is set to report the
current state rather than to fail on an incident already recorded here. Both thresholds live in
`vars` in `dbt_project.yml` and neither is written into a test's SQL.

`assert_no_settlement_without_decision` runs the reconciliation backwards, selecting settlements
with no matching decision. It returns 0 rows and it cannot return anything else on this extract,
because the source sqlite enforces a foreign key from `settlements.ticker` to
`decisions.ticker`. It is here because a feed loaded without that constraint would break in
this direction, and the forward-only tests would report a clean reconciliation while the ledger
was missing rows.

`assert_unsettled_decisions_are_classified` full outer joins `int_decision_settlement` to
`fct_reporting_breaks` and fails on an unsettled decision with no break row, a settled decision
with one, or a break row with no decision behind it. The `accepted_values` test on `reason_code`
only sees rows that reached the fact table, so a classification branch that filters a row out
instead of labelling it passes every generic test.

`assert_decisions_inside_run_window` joins each decision to the run it claims and fails when
`decided_at` falls outside that run's `started_at` to `finished_at` window. The ledger and the
run log are written by separate paths, so a decision attributed to the wrong run would move
counts between days while every count still summed correctly. It returns 0 rows across all 281
decisions. Run 11 has a null `finished_at`, an open run, and the test treats an open run as
having no upper bound rather than failing its 89 decisions.

### Do the tests work

A test that returns zero rows because its SQL is wrong is indistinguishable from a test that
returns zero rows because the data is clean. Both report green. `scripts/verify_controls.py`
tells them apart: for each control it copies the fixture, injects one violation of that control,
runs the models, then runs only that control and asserts it fails.

15 of 15 injected faults are caught by the control that names them. It runs as its own CI job on
every push and exits non-zero if any control sleeps through its fault. A full pass takes a few
minutes.

| Control | Injected fault |
|---|---|
| `unique_stg_decisions_ticker` | the same ticker decided twice |
| `unique_stg_settlements_ticker` | the settlement feed emits a ticker twice |
| `not_null_stg_decisions_occurrence_at` | a decision with no event time |
| `accepted_values` on `result` | an outcome class the models do not know |
| `accepted_values` on `fee_type` | an unpriceable fee formula published upstream |
| `accepted_range` on `mid_cents` | a quote outside the 1 to 99 contract bounds |
| `accepted_range` on `fee_rate` | a fee rate expressed as a percentage rather than a fraction |
| `relationships` on `run_id` | a decision attributed to a run the log has never heard of |
| `equal_rowcount` on the join | a duplicated settlement fanning the join out |
| `assert_no_settlement_without_decision` | the feed settles a market the ledger never traded |
| `assert_decisions_inside_run_window` | a decision timestamped outside the run it claims |
| `assert_no_unexplained_breaks` | a twelfth unexplained break, one past the backlog |
| `assert_daily_match_rate_above_floor` | a day where the feed settles almost nothing |
| `assert_unsettled_decisions_are_classified` | a branch that drops a row instead of labelling it |
| `fct_reporting_breaks` contract | a mart column whose type drifts from its declaration |

Each mutation declares the row delta it expects to produce, and a mutation that changes a
different number of rows than declared is reported as a bad fault rather than run. That check
exists because the first version of two mutations was wrong: `order by ... limit` after an
unparenthesized `union all` binds to the whole union in DuckDB, so instead of appending one row
they truncated the table to one row. One of the two then passed for the wrong reason.

A sixteenth control is inert, and the harness demonstrates it.
`relationships_int_decision_settlement_ticker__ticker__ref_stg_settlements_` selects rows
`where is_settled`, and `is_settled` is defined as "the settlement join matched", so every row
it examines has a parent by construction. Deleting a settlement a decision points at does not
make it fail, because that decision leaves the test's population. The test is kept because it
states the intent of the join, and `dbt_utils.equal_rowcount` against `stg_decisions` is the
falsifiable guard that actually protects it.

Some generic tests outside the harness are also true by construction. The `accepted_range` on
`match_rate` compares a subset count to `count(*)` over the same group, so the ratio cannot
leave 0 to 1. The `unique` on `decision_date` is guaranteed by the `group by` that builds the
daily fact. The `not_null` on `is_settled` reads a `ticker is not null` expression, which never
returns null. All three are kept because they state the intended shape of the output, and none
of them can be made to fail by any change to the data.

### Output contracts

`fct_reporting_breaks` and `fct_daily_reconciliation` both set `contract: {enforced: true}` with
a declared `data_type` on every column. dbt compares the model's actual output against that
declaration before writing the table, so a renamed column, a dropped column, or a widened type
fails the build.

The check is real rather than decorative. Changing the declared type of
`seconds_into_settlement_window` from `bigint` to `varchar` produces:

```
This model has an enforced contract that failed.
| column_name                    | definition_type | contract_type | mismatch_reason    |
| seconds_into_settlement_window | BIGINT          | VARCHAR       | data type mismatch |
```

The daily fact casts its aggregates to `integer` at the select rather than letting DuckDB return
`hugeint` for a `sum`, so the declared types stay narrow enough to mean something.

### Source freshness

The `forward_test.settlements` source declares `loaded_at_field: epoch_ms(joined_at_ms)` with
`warn_after` 12 hours and `error_after` 48 hours, so `dbt source freshness` reports how far
behind the settlement feed has fallen.

It is not in the CI gate, and it reports `ERROR STALE` when run against this extract. The newest
settlement load is 2026-08-29 20:53:54 UTC and the extract is frozen, so the gap grows by a day
every day and no threshold can pass. The thresholds are what a live feed would run under. A
control that can only ever be red is not a gate, so it is declared and run on demand rather than
enforced.

### Fee schedule snapshot

`fee_overrides_snapshot` versions the upstream fee schedule, keyed on `change_id` with a check
strategy on `fee_multiplier` and `scheduled_at`. A fee change that is repriced or moved to a new
effective date opens a second version and closes the first, so the schedule that was in force at
any past moment stays recoverable.

The upstream table keeps no history of its own. It carries 1659 rows across 1659 distinct
`change_id` and 1659 distinct `event_ticker`, one row per event, and `fee_multiplier` takes
exactly one value across all of them. Because the source keys on `change_id`, a revision to an
existing change overwrites the row rather than accumulating a second one, so the extract cannot
show whether a reprice or a reschedule ever happened. The snapshot exists to record the ones
that happen from here.

That also means there is no real version change in the data to demonstrate against, so the
snapshot is exercised against a simulated upstream change. `scripts/simulate_fee_change.py`
copies the extract to `data/simulated/`, adds 0.25 to `fee_multiplier` and 6 hours to
`scheduled_ts` on the lowest `change_id`, and leaves the other 1658 rows untouched.

```
DBT_PROFILES_DIR=. .venv/bin/dbt build
.venv/bin/python scripts/simulate_fee_change.py
DBT_PROFILES_DIR=. DBT_DATA_DIR=data/simulated .venv/bin/dbt run --select stg_fee_overrides
DBT_PROFILES_DIR=. DBT_DATA_DIR=data/simulated .venv/bin/dbt snapshot
```

The staging view has to be rebuilt against the scratch directory before the second snapshot run.
DuckDB resolves the parquet path when the view is created, so `dbt snapshot` on its own reads
the old file and records no change.

Both versions of `000d1db5-db74-426b-9637-3ed46fd59a67` after that sequence:

| `fee_multiplier` | `scheduled_at` | `dbt_valid_from` | `dbt_valid_to` |
|---|---|---|---|
| 1.0 | 2026-08-24 22:40:00 | 2026-08-31 20:17:00 | 2026-08-31 20:17:56 |
| 1.25 | 2026-08-25 04:40:00 | 2026-08-31 20:17:56 | null |

The snapshot table goes from 1659 rows to 1660 across 1659 distinct `change_id`, with 1 closed
version. This is a simulated upstream change. It is not observed drift.

## Reproduce it

`fixtures/` carries 31 KB of parquet and runs the whole project without the private source.
This is the path CI runs and it reproduces the reconciliation numbers in this README exactly,
including all six daily match rates and the 11, 6 and 1 reason-code counts. `fee_overrides` is
reduced to 50 rows in the fixture, so the fee-schedule figures come from the full extract only.

```
uv venv --python 3.12
uv pip install -r pyproject.toml
DBT_PROFILES_DIR=. .venv/bin/dbt deps
DBT_PROFILES_DIR=. DBT_DATA_DIR=fixtures .venv/bin/dbt build
```

The sqlite source is private and is not distributed here. With it in place, `scripts/extract.py`
writes the full extract to `data/`, the directory the sources read when `DBT_DATA_DIR` is unset.
Without it the script exits 1 with the path it looked for.

```
FORWARD_TEST_SQLITE=/path/to/forward_test.sqlite .venv/bin/python scripts/extract.py
DBT_PROFILES_DIR=. .venv/bin/dbt build
```

Linting needs the extra group, and the dbt templater compiles the project to lint it:

```
uv pip install -r pyproject.toml --group lint
DBT_PROFILES_DIR=. .venv/bin/sqlfluff lint models tests snapshots
```

Checking that the tests themselves work takes a few minutes and needs the fixture:

```
.venv/bin/python scripts/verify_controls.py
```

`dbt build` writes `settlement_reconciliation.duckdb` and runs 82 data tests. DuckDB reads the
parquet in `data/` through sources, so there is no warehouse account and no scheduler.
`DBT_DATA_DIR` overrides the directory the sources read from.

`dbt docs generate` writes the model graph and catalog to `target/`. The `--static` flag emits
a single self-contained `target/static_index.html`, which is what CI publishes.

Pinned in `pyproject.toml`: `dbt-core==1.12.3`, `dbt-duckdb==1.11.0`, on Python 3.12.
`dbt_utils` 1.4.1 supplies `accepted_range`.

### Continuous integration

`.github/workflows/build.yml` runs three jobs on every push and pull request, with
`DBT_DATA_DIR=fixtures`.

`lint` runs `sqlfluff lint` over `models`, `tests`, and `snapshots` with the dbt templater, so
the linter sees compiled SQL rather than Jinja. Config is in `.sqlfluff`: DuckDB dialect,
lowercase keywords, 100-column lines.

`build` runs `dbt deps`, `dbt build`, and `dbt docs generate --static`.

`verify-controls` runs `scripts/verify_controls.py`, the mutation harness described above.

`publish-docs` deploys that static file to GitHub Pages, on `main` only.

CI runs against the committed fixture, not the full extract. The private sqlite is not available
on a runner, so `fixtures/` carries 31 KB of parquet built by `scripts/build_fixture.py`.
`fee_overrides` is reduced from 1659 rows to 50, 25 per `fee_type` ordered by `change_id`, which
is enough to exercise the snapshot and the `accepted_values` test on `fee_type`.

The four reconciliation tables are copied whole, at 281 decisions, 263 settlements, 12 runs, and
1 unscoreable row. Subsampling them would move the numbers the tests exist to protect: the
settlement window is the largest close-to-load lag across all settled rows, and a daily match
rate is a ratio over a full day, so dropping rows from either changes the reason-code
distribution and the match-rate floor. The fixture reproduces 11 unexplained, 6
`settlement_pending`, 1 `unscoreable_market`, and all six daily match rates exactly.

## What this does not cover

- The 11 `unexplained` rows are not diagnosed. The extract does not contain the answer, and no
  cause is asserted for them here.
- The reverse break, a settlement with no decision, is 0 in this extract because the source
  sqlite enforces a foreign key from `settlements.ticker` to `decisions.ticker`. A test for it
  is worth having and it cannot fail on this data.
- The settlement window is inferred from observed lag with n=263 over 6 days. A feed that
  occasionally lags longer than 20.2 h would produce false `unexplained` rows, and this extract
  cannot rule that out.
- `fee_overrides` is staged, tested, and snapshotted. No model joins a realized fee back to
  the schedule that produced it, because `fee_cents` is not exported.
- The unexplained test is baselined, not zero-tolerance. See Tests above for what that buys
  and what it costs.
- The fee snapshot is exercised against a simulated upstream change. `fee_multiplier` has one
  distinct value across all 1659 rows, so the check strategy has never run against real
  variation in the column it checks.
- Source freshness is declared and cannot pass, because the extract is frozen. See Source
  freshness above.
- Reporting is not scheduled. The models run on demand. Nothing here is a live pipeline.

## License

MIT, see [LICENSE](LICENSE).
