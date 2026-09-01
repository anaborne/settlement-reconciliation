with joined as (
    select * from {{ ref('int_decision_settlement') }}
),

unscoreable as (
    select * from {{ ref('stg_unscoreable') }}
),

-- The feed carries no load-completion marker, so the newest row it produced stands in for how
-- far it has run. Anything occurring after that has had no chance to settle.
settlement_load as (
    select
        max(loaded_at) as watermark_at,
        max(date_diff('second', closed_at, loaded_at)) as max_observed_lag_seconds
    from {{ ref('stg_settlements') }}
),

classified as (
    select
        joined.ticker,
        joined.decision_date,
        joined.run_id,
        joined.ruleset_version,
        joined.series_ticker,
        joined.event_ticker,
        joined.decided_at,
        joined.occurrence_at,
        date_diff(
            'second',
            settlement_load.watermark_at
                - to_seconds(cast(settlement_load.max_observed_lag_seconds as bigint)),
            joined.occurrence_at
        ) as seconds_into_settlement_window,
        case
            when unscoreable.ticker is not null then 'unscoreable_market'
            when joined.occurrence_at >= settlement_load.watermark_at
                - to_seconds(cast(settlement_load.max_observed_lag_seconds as bigint))
                then 'settlement_pending'
            else 'unexplained'
        end as reason_code
    from joined
    cross join settlement_load
    left join unscoreable
        on joined.ticker = unscoreable.ticker
    where not joined.is_settled
)

select
    *,
    case reason_code
        when 'unexplained' then 'high'
        when 'unscoreable_market' then 'low'
        when 'settlement_pending' then 'info'
    end as severity
from classified
