-- Catches a reason-code branch that drops a row instead of labelling it. accepted_values only
-- sees rows that reached the fact table.
select
    coalesce(int_decision_settlement.ticker, fct_reporting_breaks.ticker) as ticker,
    int_decision_settlement.is_settled,
    fct_reporting_breaks.reason_code
from {{ ref('int_decision_settlement') }}
full outer join {{ ref('fct_reporting_breaks') }}
    on int_decision_settlement.ticker = fct_reporting_breaks.ticker
where
    (fct_reporting_breaks.ticker is null and not int_decision_settlement.is_settled)
    or (
        fct_reporting_breaks.ticker is not null
        and coalesce(int_decision_settlement.is_settled, true)
    )
