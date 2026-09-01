with joined as (
    select * from {{ ref('int_decision_settlement') }}
),

breaks as (
    select * from {{ ref('fct_reporting_breaks') }}
),

daily_decisions as (
    select
        decision_date,
        count(*) as decisions_in,
        sum(case when is_settled then 1 else 0 end) as settlements_matched,
        count(distinct run_id) as runs_contributing,
        string_agg(distinct ruleset_version, ', ' order by ruleset_version) as rulesets_active,
        sum(case when is_settled then net_cents else 0 end) as net_cents_settled
    from joined
    group by decision_date
),

daily_breaks as (
    select
        decision_date,
        count(*) as breaks_total,
        sum(case when reason_code = 'settlement_pending' then 1 else 0 end)
            as breaks_settlement_pending,
        sum(case when reason_code = 'unscoreable_market' then 1 else 0 end)
            as breaks_unscoreable_market,
        sum(case when reason_code = 'unexplained' then 1 else 0 end)
            as breaks_unexplained
    from breaks
    group by decision_date
)

select
    daily_decisions.decision_date,
    cast(daily_decisions.decisions_in as integer) as decisions_in,
    cast(daily_decisions.settlements_matched as integer) as settlements_matched,
    round(
        cast(daily_decisions.settlements_matched as double) / daily_decisions.decisions_in, 4
    ) as match_rate,
    cast(daily_decisions.runs_contributing as integer) as runs_contributing,
    daily_decisions.rulesets_active,
    cast(coalesce(daily_breaks.breaks_total, 0) as integer) as breaks_total,
    cast(coalesce(daily_breaks.breaks_settlement_pending, 0) as integer)
        as breaks_settlement_pending,
    cast(coalesce(daily_breaks.breaks_unscoreable_market, 0) as integer)
        as breaks_unscoreable_market,
    cast(coalesce(daily_breaks.breaks_unexplained, 0) as integer) as breaks_unexplained,
    round(daily_decisions.net_cents_settled, 2) as net_cents_settled
from daily_decisions
left join daily_breaks
    on daily_decisions.decision_date = daily_breaks.decision_date
order by daily_decisions.decision_date
