{{ config(
    alias = 'trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["arbitrum"]\',
                                "project",
                                "zigzag",
                                \'["mtitus6"]\') }}'
    )
}}

{% set project_start_date = '2022-09-01' %}

with
  dexs as (
    select
      call_block_time as block_time,
      from_hex(json_extract_scalar(zzmo.makerOrder, '$.sellToken')) as token_sold_address,
      from_hex(json_extract_scalar(zzmo.makerOrder, '$.buyToken')) as token_bought_address,
      cast(json_extract_scalar(zzmo.output_matchedFillResults, '$.takerSellFilledAmount') as UINT256) as token_bought_amount_raw,
      cast(json_extract_scalar(zzmo.output_matchedFillResults, '$.makerSellFilledAmount') as UINT256) as token_sold_amount_raw,
      NULL AS amount_usd,
      from_hex(json_extract_scalar(zzmo.makerOrder, '$.user')) as maker,
      from_hex(json_extract_scalar(zzmo.takerOrder, '$.user')) as taker,
      call_tx_hash as tx_hash,
      row_number() OVER(PARTITION BY call_tx_hash ORDER BY zzmo.makerOrder) AS evt_index, --prevent duplicates
      contract_address as project_contract_address
    from
    {{ source('zigzag_test_v6_arbitrum', 'zigzag_settelment_call_matchOrders') }} zzmo
    where
        call_success = true
        {% if is_incremental() %}
        AND zzmo.call_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
  )
SELECT
    'arbitrum' as blockchain
    ,'zigzag' as project
    ,'1' as version
    ,TRY_CAST(date_trunc('day', dexs.block_time) AS date) AS block_date
    ,CAST(date_trunc('month', dexs.block_time) AS date) AS block_month
    ,dexs.block_time
    ,erc20a.symbol AS token_bought_symbol
    ,erc20b.symbol AS token_sold_symbol
    ,case
        when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
        else concat(erc20a.symbol, '-', erc20b.symbol)
    end as token_pair
    ,dexs.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount
    ,dexs.token_sold_amount_raw / power(10, erc20b.decimals) AS token_sold_amount
    ,dexs.token_bought_amount_raw  AS token_bought_amount_raw
    ,dexs.token_sold_amount_raw  AS token_sold_amount_raw
    ,coalesce(
        dexs.amount_usd
        ,(dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
        ,(dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
    ) AS amount_usd
    ,dexs.token_bought_address
    ,dexs.token_sold_address
    ,coalesce(dexs.taker, tx."from") AS taker -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    ,dexs.maker
    ,dexs.project_contract_address
    ,dexs.tx_hash
    ,tx."from" AS tx_from
    ,tx.to AS tx_to
    ,dexs.evt_index
from dexs
INNER JOIN {{ source('arbitrum', 'transactions') }} tx
    ON tx.hash = dexs.tx_hash
    {% if not is_incremental() %}
    AND tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} erc20a
    ON erc20a.contract_address = dexs.token_bought_address 
    AND erc20a.blockchain = 'arbitrum'
LEFT JOIN {{ source('tokens', 'erc20') }} erc20b
    ON erc20b.contract_address = dexs.token_sold_address
    AND erc20b.blockchain = 'arbitrum'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    AND p_bought.blockchain = 'arbitrum'
    {% if not is_incremental() %}
    AND p_bought.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = 'arbitrum'
    {% if not is_incremental() %}
    AND p_sold.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
