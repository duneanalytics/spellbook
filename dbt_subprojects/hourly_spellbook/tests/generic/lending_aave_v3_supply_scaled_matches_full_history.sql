-- The scaled supply models are incremental, but a holder's balance depends on every supply event since
-- their first one. This test rebuilds one recent hour from full history, using the model's own macro
-- (tests never render the incremental branch), and returns every row where the stored table disagrees:
-- a holder that is missing, a holder that should not be there, or a different amount.
-- lag_hours: how far behind the current hour to compare, so that the hour is complete in the sources.
{% test lending_aave_v3_supply_scaled_matches_full_history(model, blockchain, project='aave', version='3', lag_hours=12) %}

{%- set compared_hour -%}
date_trunc('hour', now()) - interval '{{ lag_hours }}' hour
{%- endset %}

with

expected as (
  select block_hour, token_address, user, amount
  from (
    {{ lending_aave_v3_compatible_supply_scaled(blockchain = blockchain, project = project, version = version) }}
  )
  where block_hour = {{ compared_hour }}
),

actual as (
  select block_hour, token_address, user, amount
  from {{ model }}
  where block_hour = {{ compared_hour }}
)

select
  coalesce(e.block_hour, a.block_hour) as block_hour,
  coalesce(e.token_address, a.token_address) as token_address,
  coalesce(e.user, a.user) as user,
  e.amount as expected_amount,
  a.amount as actual_amount
from expected e
  full outer join actual a
    on e.block_hour = a.block_hour
    and e.token_address = a.token_address
    and e.user = a.user
where e.user is null
  or a.user is null
  or abs(e.amount - a.amount) > 1e-9 * greatest(abs(e.amount), abs(a.amount), 1)

{% endtest %}
