{% macro transfers_erc721_agg_day(transfers_erc721) %}

select
  blockchain,
  cast(date_trunc('month', block_time) as date) as block_month,
  date_trunc('day', block_time) as block_day,
  wallet_address,
  token_address,
  token_id,
  sum(amount) as amount
from {{ transfers_erc721 }}
{% if is_incremental() %}
where block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}
group by 1, 2, 3, 4, 5, 6

{% endmacro %}
