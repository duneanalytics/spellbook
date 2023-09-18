{% macro transfers_erc721_rolling_hour(transfers_erc721_agg_hour) %}

select
  blockchain,
  block_month,
  block_hour,
  wallet_address,
  token_address,
  token_id,
  sum(amount) over (partition by token_address, wallet_address, token_id order by block_hour) as amount,
  row_number() over (partition by token_address, wallet_address, token_id order by block_hour desc) as recency_index,
  now() as last_updated
from {{ transfers_erc721_agg_hour }}

{% endmacro %}
