select
    ticker,
    run_id,
    series_ticker,
    event_ticker,
    epoch_ms(decided_at_ms) as decided_at,
    epoch_ms(occurrence_ts * 1000) as occurrence_at,
    yes_bid_cents,
    yes_ask_cents,
    mid_cents,
    spread_cents,
    fee_rate,
    ruleset_version
from {{ source('forward_test', 'decisions') }}
