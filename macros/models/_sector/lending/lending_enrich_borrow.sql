{% macro lending_enrich_borrow(model) %}

select
  borrow.blockchain,
  borrow.project,
  borrow.version,
  borrow.transaction_type,
  borrow.loan_type,
  erc20.symbol,
  borrow.token_address,
  borrow.borrower,
  borrow.repayer,
  borrow.liquidator,
  borrow.amount / power(10, coalesce(erc20.decimals, 18)) as amount,
  borrow.amount / power(10, coalesce(p.decimals, erc20.decimals, 18)) * p.price as usd_amount,
  borrow.block_month,
  borrow.block_time,
  borrow.block_number,
  borrow.tx_hash,
  borrow.evt_index
from {{ model }} borrow
  left join {{ source('tokens', 'erc20') }} erc20
    on borrow.token_address = erc20.contract_address
    and borrow.blockchain = erc20.blockchain
  left join {{ source('prices', 'usd') }} p 
    on date_trunc('minute', borrow.block_time) = p.minute
    and borrow.token_address = p.contract_address
    and borrow.blockchain = p.blockchain
    {% if is_incremental() %}
    and {{ incremental_predicate('p.minute') }}
    {% endif %}
{% if is_incremental() %}
where {{ incremental_predicate('borrow.block_time') }}
{% endif %}

{% endmacro %}
