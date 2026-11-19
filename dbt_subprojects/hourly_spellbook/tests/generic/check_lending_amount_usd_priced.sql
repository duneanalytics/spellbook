-- Fails for each recent row that has no amount_usd although prices.minute has a price for its token at that minute.
-- The window sits inside the 3-day incremental lookback, so a scheduled run re-prices every row it covers.
-- It ends 6 hours back because prices.minute trails the chain by about an hour.
{% test check_lending_amount_usd_priced(model) %}

select
  m.blockchain,
  m.project,
  m.version,
  m.token_address,
  m.block_time,
  m.tx_hash,
  m.evt_index
from {{ model }} m
  inner join {{ source('prices', 'minute') }} p
    on date_trunc('minute', m.block_time) = p.timestamp
    and m.token_address = p.contract_address
    and m.blockchain = p.blockchain
    and p.timestamp >= now() - interval '3' day
    and p.timestamp < now() - interval '6' hour
where m.amount_usd is null
  and m.amount_raw is not null
  and p.price is not null
  and m.block_time >= now() - interval '2' day
  and m.block_time < now() - interval '6' hour

{% endtest %}
