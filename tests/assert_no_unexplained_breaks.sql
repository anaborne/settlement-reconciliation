-- Errors above the accepted backlog rather than on any row, so a build reports the size of the
-- unexplained population instead of stalling on breaks already recorded in the README.
{{ config(
    warn_if = '>0',
    error_if = '>' ~ var('unexplained_break_baseline')
) }}

select
    ticker,
    decision_date,
    series_ticker,
    seconds_into_settlement_window
from {{ ref('fct_reporting_breaks') }}
where reason_code = 'unexplained'
