-- An hour's liquidity index and variable borrow index must be the values of the last reserve update
-- in that hour, by block number and event index. This test derives them from the event-level market
-- model and returns every hour where the hourly model stores something else.
-- lag_hours: the most recent hours are left out, because they can still receive updates.
{% test lending_market_hourly_agg_holds_closing_index(model, market_model, lag_hours=12) %}

with closing as (
  select
    block_hour,
    token_address,
    max_by(liquidity_index, (block_number, evt_index)) as liquidity_index,
    max_by(variable_borrow_index, (block_number, evt_index)) as variable_borrow_index
  from {{ market_model }}
  group by 1, 2
)

select
  h.block_hour,
  h.token_address,
  c.liquidity_index as closing_liquidity_index,
  h.liquidity_index as stored_liquidity_index,
  c.variable_borrow_index as closing_variable_borrow_index,
  h.variable_borrow_index as stored_variable_borrow_index
from {{ model }} h
  inner join closing c
    on h.block_hour = c.block_hour
    and h.token_address = c.token_address
where h.block_hour < date_trunc('hour', now()) - interval '{{ lag_hours }}' hour
  and (
    h.liquidity_index <> c.liquidity_index
    or h.variable_borrow_index <> c.variable_borrow_index
  )

{% endtest %}
