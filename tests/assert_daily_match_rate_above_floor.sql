select
    decision_date,
    decisions_in,
    settlements_matched,
    match_rate
from {{ ref('fct_daily_reconciliation') }}
where match_rate < {{ var('min_daily_match_rate') }}
