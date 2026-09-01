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
    daily_decisions.decisions_in,
    daily_decisions.settlements_matched,
    round(
        cast(daily_decisions.settlements_matched as double) / daily_decisions.decisions_in, 4
    ) as match_rate,
    coalesce(daily_breaks.breaks_total, 0) as breaks_total,
    coalesce(daily_breaks.breaks_settlement_pending, 0) as breaks_settlement_pending,
    coalesce(daily_breaks.breaks_unscoreable_market, 0) as breaks_unscoreable_market,
    coalesce(daily_breaks.breaks_unexplained, 0) as breaks_unexplained,
    round(daily_decisions.net_cents_settled, 2) as net_cents_settled
from daily_decisions
left join daily_breaks
    on daily_decisions.decision_date = daily_breaks.decision_date
order by daily_decisions.decision_date
