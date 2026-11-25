-- The hourly market models are incremental and forward-fill hours without a reserve update from the
-- last update before them, which can lie before the incremental window. This test rebuilds the hours
-- of the incremental window from full history, using the model's own macro (tests never render the
-- incremental branch), and returns every hour where the stored table disagrees.
-- lag_hours: the most recent hours are left out, because they can still receive updates.
{% test lending_aave_v3_market_hourly_agg_matches_full_history(model, blockchain, project='aave', version='3', lag_hours=12) %}

{%- set compared_hours -%}
{{ incremental_predicate('block_hour') }} and block_hour < date_trunc('hour', now()) - interval '{{ lag_hours }}' hour
{%- endset %}

with

expected as (
  select block_hour, token_address, liquidity_index, variable_borrow_index, deposit_rate, stable_borrow_rate, variable_borrow_rate
  from (
    {{ lending_aave_v3_compatible_market_hourly_agg(blockchain = blockchain, project = project, version = version) }}
  )
  where {{ compared_hours }}
),

actual as (
  select block_hour, token_address, liquidity_index, variable_borrow_index, deposit_rate, stable_borrow_rate, variable_borrow_rate
  from {{ model }}
  where {{ compared_hours }}
)

select
  coalesce(e.block_hour, a.block_hour) as block_hour,
  coalesce(e.token_address, a.token_address) as token_address,
  e.liquidity_index as expected_liquidity_index,
  a.liquidity_index as actual_liquidity_index,
  e.deposit_rate as expected_deposit_rate,
  a.deposit_rate as actual_deposit_rate,
  e.variable_borrow_rate as expected_variable_borrow_rate,
  a.variable_borrow_rate as actual_variable_borrow_rate
from expected e
  full outer join actual a
    on e.block_hour = a.block_hour
    and e.token_address = a.token_address
where e.token_address is null
  or a.token_address is null
  or e.liquidity_index is distinct from a.liquidity_index
  or e.variable_borrow_index is distinct from a.variable_borrow_index
  or abs(coalesce(e.deposit_rate, 0) - coalesce(a.deposit_rate, 0)) > 1e-12
  or abs(coalesce(e.stable_borrow_rate, 0) - coalesce(a.stable_borrow_rate, 0)) > 1e-12
  or abs(coalesce(e.variable_borrow_rate, 0) - coalesce(a.variable_borrow_rate, 0)) > 1e-12

{% endtest %}
