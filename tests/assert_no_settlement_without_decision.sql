select
    stg_settlements.ticker,
    stg_settlements.result,
    stg_settlements.loaded_at
from {{ ref('stg_settlements') }}
left join {{ ref('stg_decisions') }}
    on stg_settlements.ticker = stg_decisions.ticker
where stg_decisions.ticker is null
