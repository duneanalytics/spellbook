{{
    config(
        tags=['dunesql'],
        schema = 'mento_celo',
        alias = alias('trades_v1'),
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'tx_hash', 'evt_index'],
        post_hook = '{{ expose_spells(\'["celo"]\',
                                    "project",
                                    "mento",
                                    \'["tomfutago"]\') }}'
    )
}}

{% set project_start_date = '2020-04-22' %}

{% set dexs %}
  --Mento v1
  select
    t.evt_block_time as block_time,
    t.exchanger as taker,
    cast(null as varbinary) as maker,
    t.buyAmount as token_bought_amount_raw,
    t.sellAmount as token_sold_amount_raw,
    null as amount_usd,
    case
      when t.soldGold then 0x765DE816845861e75A25fCA122bb6898B8B1282a -- cUSD
      else 0x471EcE3750Da237f93B8E339c536989b8978a438                 -- CELO
    end as token_bought_address,
    case
      when t.soldGold then 0x471EcE3750Da237f93B8E339c536989b8978a438 -- CELO
      else 0x765DE816845861e75A25fCA122bb6898B8B1282a                 -- cUSD
    end as token_sold_address,
    t.contract_address as project_contract_address,
    t.evt_tx_hash as tx_hash,
    t.evt_index
  from {{ source('mento_celo', 'Exchange_evt_Exchanged') }} t
  {% if is_incremental() %}
  where t.evt_block_time >= date_trunc('day', now() - interval '7' day)
  {% endif %}

  -- TODO: add 2 other pairs
{% endset %}

select distinct
  'celo' as blockchain,
  'mento' as project,
  '1' as version,
  cast(date_trunc('month', dexs.block_time) as date) as block_month,
  cast(date_trunc('day', dexs.block_time) as date) as block_date,
  dexs.block_time,
  erc20a.symbol as token_bought_symbol,
  erc20b.symbol as token_sold_symbol,
  case
    when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
    else concat(erc20a.symbol, '-', erc20b.symbol)
  end as token_pair,
  dexs.token_bought_amount_raw / power(10, erc20a.decimals) as token_bought_amount,
  dexs.token_sold_amount_raw / power(10, erc20b.decimals) as token_sold_amount,
  cast(dexs.token_bought_amount_raw as uint256) as token_bought_amount_raw,
  cast(dexs.token_sold_amount_raw as uint256) as token_sold_amount_raw,
  coalesce(
    dexs.amount_usd,
    (dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price,
    (dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
  ) as amount_usd,
  dexs.token_bought_address,
  dexs.token_sold_address,
  coalesce(dexs.taker, tx."from") as taker, -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
  dexs.maker,
  dexs.project_contract_address,
  dexs.tx_hash,
  tx."from" as tx_from,
  tx.to as tx_to,
  dexs.evt_index
from dexs
  join {{ source('celo', 'transactions') }} tx on tx.hash = dexs.tx_hash
    {% if not is_incremental() %}
    and tx.block_time >= timestamp '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    and tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
  left join {{ ref('tokens_erc20') }} erc20a
    on erc20a.contract_address = dexs.token_bought_address
    and erc20a.blockchain = 'celo'
  left join {{ ref('tokens_erc20') }} erc20b
    on erc20b.contract_address = dexs.token_sold_address
    and erc20b.blockchain = 'celo'
  left join {{ source('prices', 'usd') }} p_bought
    on p_bought.minute = date_trunc('minute', dexs.block_time)
    and p_bought.contract_address = dexs.token_bought_address
    and p_bought.blockchain = 'celo'
    {% if not is_incremental() %}
    and p_bought.minute >= timestamp '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    and p_bought.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
  left join {{ source('prices', 'usd') }} p_sold
    on p_sold.minute = date_trunc('minute', dexs.block_time)
    and p_sold.contract_address = dexs.token_sold_address
    and p_sold.blockchain = 'celo'
    {% if not is_incremental() %}
    and p_sold.minute >= timestamp '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    and p_sold.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
