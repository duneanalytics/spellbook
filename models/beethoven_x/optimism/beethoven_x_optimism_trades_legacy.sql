{{ config(
	tags=['legacy'],
	
    schema = 'beethoven_x_optimism',
    alias = alias('trades', legacy_model=True),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "beethoven_x",
                                \'["msilb7"]\') }}'
    )
}}
-- First swap event 2022-05-23
{% set project_start_date = '2022-05-23' %}

WITH dexs AS
(
    -- Migrated from: https://github.com/duneanalytics/dune-v1-abstractions/blob/main/deprecated-dune-v1-abstractions/optimism2/dex/insert_beethoven_x.sql
    SELECT
         t.evt_block_time AS block_time
        , t.evt_block_number
        ,'' AS taker --not in the event table, so we rely on the transaction "from"
        ,'' AS maker
        --tokenIn: what the user receives. So we map this to token bought
        ,t.amountIn AS token_bought_amount_raw
        ,t.amountOut AS token_sold_amount_raw
        ,cast(NULL as double) AS amount_usd
        ,t.tokenIn AS token_bought_address
        ,t.tokenOut AS token_sold_address
        ,t.poolId as project_contract_address
        ,t.evt_tx_hash AS tx_hash
        ,'' AS trace_address
        ,t.evt_index
    FROM
        {{ source('balancer_v2_optimism', 'Vault_evt_Swap') }} t
    WHERE t.tokenIn != substring(t.poolId, 1, 20)
        AND t.tokenOut != substring(t.poolId, 1, 20)
    {% if is_incremental() %}
    AND t.evt_block_time >= date_trunc('day', now() - interval '1 week')
    {% endif %}
)
SELECT
     'optimism' AS blockchain
    ,'beethoven_x' AS project
    ,'2' AS version
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
    ,tx.from AS taker -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
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