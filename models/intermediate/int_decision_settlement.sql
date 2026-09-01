with decisions as (
    select * from {{ ref('stg_decisions') }}
),

settlements as (
    select * from {{ ref('stg_settlements') }}
)

select
    decisions.ticker,
    decisions.run_id,
    decisions.ruleset_version,
    decisions.series_ticker,
    decisions.event_ticker,
    decisions.decided_at,
    cast(decisions.decided_at as date) as decision_date,
    decisions.occurrence_at,
    decisions.mid_cents,
    decisions.spread_cents,
    decisions.fee_rate,
    settlements.result,
    settlements.closed_at,
    settlements.net_cents,
    settlements.loaded_at,
    settlements.ticker is not null as is_settled
from decisions
left join settlements
    on decisions.ticker = settlements.ticker
