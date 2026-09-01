-- The ledger and the run log are written by separate paths. A decision timestamped outside the
-- window of the run it claims puts every per-run and per-day figure downstream in doubt.
select
    stg_decisions.ticker,
    stg_decisions.run_id,
    stg_decisions.decided_at,
    stg_runs.started_at,
    stg_runs.finished_at
from {{ ref('stg_decisions') }}
inner join {{ ref('stg_runs') }}
    on stg_decisions.run_id = stg_runs.run_id
where
    stg_decisions.decided_at < stg_runs.started_at
    or stg_decisions.decided_at > coalesce(stg_runs.finished_at, stg_decisions.decided_at)
