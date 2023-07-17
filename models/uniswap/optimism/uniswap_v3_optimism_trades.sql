{{ config(
    schema = 'uniswap_v3_optimism',
    alias = alias('trades'),
    tags = ['dunesql'],
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "uniswap_v3",
                                \'["jeff-dude", "markusbkoch", "masquot", "milkyklim", "0xBoxer", "mewwts", "hagaetc"]\') }}'
    )
}}
-- OVM 1 Launch 06-23-21
{% set project_start_date = '2021-06-23' %}

WITH dexs AS
(
    --Uniswap v3
    SELECT
        t.evt_block_time AS block_time
        , t.evt_block_number
        , t.recipient AS taker
        ,CAST(NULL AS varbinary) AS maker
        ,CASE WHEN amount0 < cast(0 as int256) THEN abs(amount0) ELSE abs(amount1) END AS token_bought_amount_raw -- when amount0 is negative it means trader_a is buying token0 from the pool
        ,CASE WHEN amount0 < cast(0 as int256) THEN abs(amount1) ELSE abs(amount0) END AS token_sold_amount_raw
        ,NULL AS amount_usd
        ,LOWER(CASE WHEN amount0 < cast(0 as int256) THEN f.token0 ELSE f.token1 END) AS token_bought_address
        ,LOWER(CASE WHEN amount0 < cast(0 as int256) THEN f.token1 ELSE f.token0 END) AS token_sold_address
        ,CAST(t.contract_address as string) as project_contract_address
        ,t.evt_tx_hash AS tx_hash
        ,'' AS trace_address
        ,t.evt_index
    FROM
        {{ source('uniswap_v3_optimism', 'Pair_evt_Swap') }} t
    INNER JOIN {{ ref('uniswap_optimism_pools') }} f
        ON f.pool = t.contract_address
    {% if is_incremental() %}
    WHERE t.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)
SELECT
    'optimism' AS blockchain
    ,'uniswap' AS project
    ,'3' AS version
    ,TRY_CAST(date_trunc('DAY', dexs.block_time) AS date) AS block_date
    ,dexs.block_time
    ,erc20a.symbol AS token_bought_symbol
    ,erc20b.symbol AS token_sold_symbol
    ,case
        when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
        else concat(erc20a.symbol, '-', erc20b.symbol)
    end as token_pair
    ,cast( dexs.token_bought_amount_raw as double) / cast( power(10, erc20a.decimals) as double) AS token_bought_amount
    ,cast( dexs.token_sold_amount_raw as double) / cast( power(10, erc20b.decimals) as double) AS token_sold_amount
    ,CAST(dexs.token_bought_amount_raw AS double) AS token_bought_amount_raw
    ,CAST(dexs.token_sold_amount_raw AS double) AS token_sold_amount_raw
    ,coalesce(
        dexs.amount_usd
        ,(cast( dexs.token_bought_amount_raw as double) / cast( power(10, p_bought.decimals) as double)) * p_bought.price
        ,(cast( dexs.token_sold_amount_raw as double) / cast( power(10, p_sold.decimals) as double)) * p_sold.price
    ) AS amount_usd
    ,dexs.token_bought_address
    ,dexs.token_sold_address
    ,coalesce(dexs.taker, tx.from) AS taker -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    ,dexs.maker
    ,dexs.project_contract_address
    ,dexs.tx_hash
    ,tx.from AS tx_from
    ,tx.to AS tx_to
    ,dexs.trace_address
    ,dexs.evt_index
FROM dexs
INNER JOIN {{ source('optimism', 'transactions') }} tx
    ON tx.hash = dexs.tx_hash
    AND tx.block_number = dexs.evt_block_number
    {% if not is_incremental() %}
    AND tx.block_time >= cast('{{project_start_date}}' as timestamp)
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ ref('tokens_erc20') }} erc20a
    ON erc20a.contract_address = dexs.token_bought_address 
    AND erc20a.blockchain = 'optimism'
LEFT JOIN {{ ref('tokens_erc20') }} erc20b
    ON erc20b.contract_address = dexs.token_sold_address
    AND erc20b.blockchain = 'optimism'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    AND p_bought.blockchain = 'optimism'
    {% if not is_incremental() %}
    AND p_bought.minute >= cast('{{project_start_date}}' as timestamp)
    {% endif %}
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = 'optimism'
    {% if not is_incremental() %}
    AND p_sold.minute >= cast('{{project_start_date}}' as timestamp)
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
;