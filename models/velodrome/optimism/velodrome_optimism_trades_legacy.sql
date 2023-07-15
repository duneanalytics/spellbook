{{ config(
	tags=['legacy'],
	
    alias = alias('trades', legacy_model=True),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "velodrome",
                                \'["msilb7"]\') }}'
    )
}}
-- derived from
-- SELECT MIN(evt_block_time) FROM velodrome_optimism.Pair_evt_Swap
{% set project_start_date = '2022-06-01' %}

WITH dexs AS
(
    SELECT
        '1' as version
        ,t.evt_block_time AS block_time
        ,t.evt_block_number
        ,t.to AS taker
        ,'' AS maker
        -- logic from ethereum/dex/trades/insert_uniswap_v2
	    ,CASE WHEN amount0Out = '0' THEN amount1Out ELSE amount0Out END AS token_bought_amount_raw -- when amount0 is negative it means trader_a is buying token0 from the pool
	    ,CASE WHEN amount0In = '0' OR amount1Out = '0' THEN amount1In ELSE amount0In END AS token_sold_amount_raw
        ,NULL AS amount_usd
        ,CASE WHEN amount0Out = '0' THEN token1 ELSE token0 END AS token_bought_address
	    ,CASE WHEN amount0In = '0' OR amount1Out = '0' THEN token1 ELSE token0 END AS token_sold_address
        ,CAST(t.contract_address as string) as project_contract_address
        ,t.evt_tx_hash AS tx_hash
        ,'' AS trace_address
        ,t.evt_index
    FROM
        {{ source('velodrome_optimism', 'Pair_evt_Swap') }} t
    INNER JOIN {{ source('velodrome_optimism', 'PairFactory_evt_PairCreated') }} f
        ON f.pair = t.contract_address
    {% if is_incremental() %}
    WHERE t.evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}

    UNION ALL

    SELECT
        '2' as version
        ,t.evt_block_time AS block_time
        ,t.evt_block_number
        ,t.to AS taker
        ,'' AS maker
        -- logic from ethereum/dex/trades/insert_uniswap_v2
	    ,CASE WHEN amount0Out = '0' THEN amount1Out ELSE amount0Out END AS token_bought_amount_raw -- when amount0 is negative it means trader_a is buying token0 from the pool
	    ,CASE WHEN amount0In = '0' OR amount1Out = '0' THEN amount1In ELSE amount0In END AS token_sold_amount_raw
        ,NULL AS amount_usd
        ,CASE WHEN amount0Out = '0' THEN token1 ELSE token0 END AS token_bought_address
	    ,CASE WHEN amount0In = '0' OR amount1Out = '0' THEN token1 ELSE token0 END AS token_sold_address
        ,CAST(t.contract_address as string) as project_contract_address
        ,t.evt_tx_hash AS tx_hash
        ,'' AS trace_address
        ,t.evt_index
    FROM
        {{ source('velodrome_v2_optimism', 'Pool_evt_Swap') }} t
    INNER JOIN {{ source('velodrome_v2_optimism', 'PoolFactory_evt_PoolCreated') }} f
        ON f.pool = t.contract_address
    {% if is_incremental() %}
    WHERE t.evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
)
SELECT
    'optimism' AS blockchain
    ,'velodrome' AS project
    ,version
    ,TRY_CAST(date_trunc('DAY', dexs.block_time) AS date) AS block_date
    ,dexs.block_time
    ,erc20a.symbol AS token_bought_symbol
    ,erc20b.symbol AS token_sold_symbol
    ,case
        when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
        else concat(erc20a.symbol, '-', erc20b.symbol)
    end as token_pair
    ,dexs.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount
    ,dexs.token_sold_amount_raw / power(10, erc20b.decimals) AS token_sold_amount
    ,CAST(dexs.token_bought_amount_raw AS DECIMAL(38,0)) AS token_bought_amount_raw
    ,CAST(dexs.token_sold_amount_raw AS DECIMAL(38,0)) AS token_sold_amount_raw
    ,coalesce(
        dexs.amount_usd
        ,(dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
        ,(dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
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
    AND tx.block_time >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '1' week)
    {% endif %}
LEFT JOIN {{ ref('tokens_erc20_legacy') }} erc20a
    ON erc20a.contract_address = dexs.token_bought_address 
    AND erc20a.blockchain = 'optimism'
LEFT JOIN {{ ref('tokens_erc20_legacy') }} erc20b
    ON erc20b.contract_address = dexs.token_sold_address
    AND erc20b.blockchain = 'optimism'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    AND p_bought.blockchain = 'optimism'
    {% if not is_incremental() %}
    AND p_bought.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc('day', now() - interval '1 week')
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = 'optimism'
    {% if not is_incremental() %}
    AND p_sold.minute >= '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc('day', now() - interval '1 week')
    {% endif %}
;