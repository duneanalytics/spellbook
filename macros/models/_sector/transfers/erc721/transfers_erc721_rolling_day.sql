{% macro transfers_erc721_rolling_day(transfers_erc721_agg_day) %}

select
  blockchain,
  block_month,
  block_day,
  wallet_address,
  token_address,
  token_id,
  sum(amount) over (partition by token_address, wallet_address, token_id order by block_day) as amount,
  row_number() over (partition by token_address, wallet_address, token_id order by block_day desc) as recency_index,
  now() as last_updated
from {{ transfers_erc721_agg_day }}

{% endmacro %}
