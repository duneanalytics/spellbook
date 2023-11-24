{% macro lending_enrich_flashloans(model) %}

select
  flashloans.blockchain,
  flashloans.project,
  flashloans.version,
  flashloans.currency_contract,
  erc20.symbol,
  flashloans.recipient,
  flashloans.amount / power(10, coalesce(erc20.decimals, 18)) as amount,
  flashloans.amount / power(10, coalesce(p.decimals, erc20.decimals, 18)) * p.price as usd_amount,
  flashloans.fee / power(10, coalesce(erc20.decimals, 18)) as fee,
  flashloans.block_month,
  flashloans.block_time,
  flashloans.block_number,
  flashloans.tx_hash,
  flashloans.evt_index
from {{ model }} flashloans
  left join {{ ref('tokens_erc20') }} erc20
    on flashloans.currency_contract = erc20.contract_address
    and flashloans.blockchain = erc20.blockchain
  left join {{ source('prices', 'usd') }} p 
    on date_trunc('minute', flashloans.block_time) = p.minute
    and erc20.symbol = p.symbol
    and flashloans.currency_contract = p.contract_address
    and flashloans.blockchain = p.blockchain
    {% if is_incremental() %}
    and {{ incremental_predicate('p.minute') }}
    {% endif %}

{% endmacro %}
