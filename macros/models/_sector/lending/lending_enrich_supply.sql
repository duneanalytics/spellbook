{% macro lending_enrich_supply(model) %}

select
  supply.blockchain,
  supply.project,
  supply.version,
  supply.transaction_type,
  erc20.symbol,
  supply.token_address,
  supply.depositor,
  supply.withdrawn_to,
  supply.liquidator,
  supply.amount / power(10, coalesce(erc20.decimals, 18)) as amount,
  supply.amount / power(10, coalesce(p.decimals, erc20.decimals, 18)) * p.price as usd_amount,
  supply.block_month,
  supply.block_time,
  supply.block_number,
  supply.tx_hash,
  supply.evt_index
from {{ model }} supply
  left join {{ source('tokens', 'erc20') }} erc20
    on supply.token_address = erc20.contract_address
    and supply.blockchain = erc20.blockchain
  left join {{ source('prices', 'usd') }} p 
    on date_trunc('minute', supply.block_time) = p.minute
    and supply.token_address = p.contract_address
    and supply.blockchain = p.blockchain
    {% if is_incremental() %}
    and {{ incremental_predicate('p.minute') }}
    {% endif %}
{% if is_incremental() %}
where {{ incremental_predicate('supply.block_time') }}
{% endif %}

{% endmacro %}
