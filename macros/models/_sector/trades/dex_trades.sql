{% macro dex_trades(blockchain, project, version, project_start_date, dex) %}

select distinct
  '{{blockchain}}' as blockchain,
  '{{project}}' as project,
  '{{version}}' as version,
  cast(date_trunc('month', dex.block_time) as date) as block_month,
  cast(date_trunc('day', dex.block_time) as date) as block_date,
  dex.block_time,
  erc20a.symbol as token_bought_symbol,
  erc20b.symbol as token_sold_symbol,
  case
    when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
    else concat(erc20a.symbol, '-', erc20b.symbol)
  end as token_pair,
  dex.token_bought_amount_raw / power(10, erc20a.decimals) as token_bought_amount,
  dex.token_sold_amount_raw / power(10, erc20b.decimals) as token_sold_amount,
  cast(dex.token_bought_amount_raw as uint256) as token_bought_amount_raw,
  cast(dex.token_sold_amount_raw as uint256) as token_sold_amount_raw,
  coalesce(
    dex.amount_usd,
    (dex.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price,
    (dex.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
  ) as amount_usd,
  dex.token_bought_address,
  dex.token_sold_address,
  coalesce(dex.taker, tx."from") as taker, -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
  dex.maker,
  dex.project_contract_address,
  dex.tx_hash,
  tx."from" as tx_from,
  tx.to as tx_to,
  dex.evt_index
from {{ dex }} dex
  join {{ source('{{blockchain}}', 'transactions') }} tx on tx.hash = dex.tx_hash
    {% if not is_incremental() %}
    and tx.block_time >= timestamp '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    and tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
  left join {{ ref('tokens_erc20') }} erc20a
    on erc20a.contract_address = dex.token_bought_address
    and erc20a.blockchain = '{{blockchain}}'
  left join {{ ref('tokens_erc20') }} erc20b
    on erc20b.contract_address = dex.token_sold_address
    and erc20b.blockchain = '{{blockchain}}'
  left join {{ source('prices', 'usd') }} p_bought
    on p_bought.minute = date_trunc('minute', dex.block_time)
    and p_bought.contract_address = dex.token_bought_address
    and p_bought.blockchain = '{{blockchain}}'
    {% if not is_incremental() %}
    and p_bought.minute >= timestamp '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    and p_bought.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
  left join {{ source('prices', 'usd') }} p_sold
    on p_sold.minute = date_trunc('minute', dex.block_time)
    and p_sold.contract_address = dex.token_sold_address
    and p_sold.blockchain = '{{blockchain}}'
    {% if not is_incremental() %}
    and p_sold.minute >= timestamp '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    and p_sold.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}

{% endmacro %}
