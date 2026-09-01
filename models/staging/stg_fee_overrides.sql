select
    change_id,
    event_ticker,
    series_ticker,
    fee_type,
    fee_multiplier,
    epoch_ms(scheduled_ts * 1000) as scheduled_at,
    epoch_ms(first_seen_ms) as first_seen_at
from {{ source('forward_test', 'fee_overrides') }}
