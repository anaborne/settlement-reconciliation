select
    ticker,
    result,
    epoch_ms(close_ts * 1000) as closed_at,
    net_cents,
    epoch_ms(joined_at_ms) as loaded_at
from {{ source('forward_test', 'settlements') }}
