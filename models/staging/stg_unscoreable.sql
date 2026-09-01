select
    ticker,
    result as unscoreable_result,
    status as unscoreable_status
from {{ source('forward_test', 'unscoreable') }}
