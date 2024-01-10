{{
    config(
        schema = 'curvefi_celo',
        alias = 'trades',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    )
}}

{% set project_start_date = '2022-04-27' %}

with dexs as (
  -- Stableswap
  select
    'stable' as pool_type,
    t.evt_block_time as block_time,
    t.evt_block_number as block_number,
    t.buyer as taker,
    cast(null as varbinary) as maker,
    t.tokens_bought as token_bought_amount_raw,
    t.tokens_sold as token_sold_amount_raw,
    pool_bought.token as token_bought_address,
    pool_sold.token as token_sold_address,
    t.contract_address as project_contract_address,
    t.evt_tx_hash as tx_hash,
    t.evt_index,
    t.bought_id,
    t.sold_id
  from {{ source('curvefi_celo', 'StableSwap_evt_TokenExchange') }} t
    join {{ ref('curvefi_celo_pools') }} pool_bought on t.contract_address = pool_bought.pool and t.bought_id = pool_bought.tokenid
    join {{ ref('curvefi_celo_pools') }} pool_sold on t.contract_address = pool_sold.pool and t.sold_id = pool_sold.tokenid
  {% if is_incremental() %}
  where {{ incremental_predicate('t.evt_block_time') }}
  {% endif %}
)

select
  'celo' as blockchain,
  'curve' as project,
  '1' as version,
  cast(date_trunc('day', dexs.block_time) as date) as block_date,
  cast(date_trunc('month', dexs.block_time) as date) as block_month,
  dexs.block_time,
  erc20a.symbol as token_bought_symbol,
  erc20b.symbol as token_sold_symbol,
  case
    when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
    else concat(erc20a.symbol, '-', erc20b.symbol)
  end as token_pair,
  dexs.token_bought_amount_raw / power(10, coalesce(erc20a.decimals, 18)) as token_bought_amount,
  dexs.token_sold_amount_raw / power(10, coalesce(erc20b.decimals, 18)) as token_sold_amount,
  dexs.token_bought_amount_raw  as token_bought_amount_raw,
  dexs.token_sold_amount_raw  as token_sold_amount_raw,
  coalesce(
    (dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price,
    (dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
  ) as amount_usd,
  dexs.token_bought_address,
  dexs.token_sold_address,
  coalesce(dexs.taker, tx."from") as taker,
  dexs.maker,
  dexs.project_contract_address,
  dexs.tx_hash,
  tx."from" as tx_from,
  tx.to as tx_to,
  dexs.evt_index
from dexs
  inner join {{ source('celo', 'transactions') }} tx
    on dexs.tx_hash = tx.hash
    {% if not is_incremental() %}
    and tx.block_time >= timestamp '{{project_start_date}}'
    {% else %}
    and {{ incremental_predicate('tx.block_time') }}
    {% endif %}
  left join {{ source('tokens', 'erc20') }} erc20a
    on erc20a.contract_address = dexs.token_bought_address
    and erc20a.blockchain = 'celo'
  left join {{ source('tokens', 'erc20') }} erc20b
    on erc20b.contract_address = dexs.token_sold_address
    and erc20b.blockchain = 'celo'
  left join {{ source('prices', 'usd') }} p_bought
    on p_bought.minute = date_trunc('minute', dexs.block_time)
    and p_bought.contract_address = dexs.token_bought_address
    and p_bought.blockchain = 'celo'
    {% if not is_incremental() %}
    and p_bought.minute >= timestamp '{{project_start_date}}'
    {% else %}
    and {{ incremental_predicate('p_bought.minute') }}
    {% endif %}
  left join {{ source('prices', 'usd') }} p_sold
    on p_sold.minute = date_trunc('minute', dexs.block_time)
    and p_sold.contract_address = dexs.token_sold_address
    and p_sold.blockchain = 'celo'
    {% if not is_incremental() %}
    and p_sold.minute >= timestamp '{{project_start_date}}'
    {% else %}
    and {{ incremental_predicate('p_sold.minute') }}
    {% endif %}
