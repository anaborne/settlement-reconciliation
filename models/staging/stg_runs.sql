select
    run_id,
    epoch_ms(started_at_ms) as started_at,
    epoch_ms(finished_at_ms) as finished_at,
    ruleset_version
from {{ source('forward_test', 'runs') }}
