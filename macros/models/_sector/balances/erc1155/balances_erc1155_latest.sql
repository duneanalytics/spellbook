{% macro balances_erc1155_latest(balances_erc1155_hour) %}

select
  blockchain,
  token_address,
  token_id,
  collection,
  max_by(amount, block_hour) as amount,
  max_by(wallet_address, block_hour) as wallet_address,
  max(block_hour) as last_updated
from {{ balances_erc1155_hour }}
group by 1, 2, 3, 4

{% endmacro %}
