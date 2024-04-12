{{ config(
    schema = 'beethoven_x_fantom',
    alias = 'trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["fantom"]\',
                                "project",
                                "beethoven_x",
                                \'["msilb7", "tomfutago"]\') }}'
    )
}}
-- First swap event 2021-01-19
{% set project_start_date = '2021-01-19' %}

WITH dexs AS
(
    -- Migrated from: https://github.com/duneanalytics/dune-v1-abstractions/blob/main/deprecated-dune-v1-abstractions/fantom2/dex/insert_beethoven_x.sql
    SELECT
         t.evt_block_time AS block_time
        , t.evt_block_number
        ,CAST(NULL AS VARBINARY) AS taker --not in the event table, so we rely on the transaction "from"
        ,CAST(NULL AS VARBINARY) AS maker
        --tokenIn: what the user receives. So we map this to token bought
        ,t.amountOut AS token_bought_amount_raw
        ,t.amountIn AS token_sold_amount_raw
        ,NULL AS amount_usd
        ,t.tokenOut AS token_bought_address
        ,t.tokenIn AS token_sold_address
        ,bytearray_substring(t.poolId, 1, 20) as project_contract_address
        ,t.evt_tx_hash AS tx_hash
        ,t.evt_index
    FROM
        {{ source('beethoven_x_fantom', 'Vault_evt_Swap') }} t
    WHERE t.tokenIn != bytearray_substring(t.poolId, 1, 20)
        AND t.tokenOut != bytearray_substring(t.poolId, 1, 20)
    {% if is_incremental() %}
    AND {{ incremental_predicate('t.evt_block_time') }}
    {% endif %}
)
SELECT
     'fantom' AS blockchain
    ,'beethoven_x' AS project
    ,'1' AS version
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
    ,tx."from" AS taker -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    ,dexs.maker
    ,dexs.project_contract_address
    ,dexs.tx_hash
    ,tx."from" AS tx_from
    ,tx.to AS tx_to
    ,dexs.evt_index
FROM dexs
INNER JOIN {{ source('fantom', 'transactions') }} tx
    ON tx.hash = dexs.tx_hash
    AND tx.block_number = dexs.evt_block_number
    {% if not is_incremental() %}
    AND tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND {{ incremental_predicate('tx.block_time') }}
    {% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} erc20a
    ON erc20a.contract_address = dexs.token_bought_address 
    AND erc20a.blockchain = 'fantom'
LEFT JOIN {{ source('tokens', 'erc20') }} erc20b
    ON erc20b.contract_address = dexs.token_sold_address
    AND erc20b.blockchain = 'fantom'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    AND p_bought.blockchain = 'fantom'
    {% if not is_incremental() %}
    AND p_bought.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND {{ incremental_predicate('p_bought.minute') }}
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = 'fantom'
    {% if not is_incremental() %}
    AND p_sold.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND {{ incremental_predicate('p_sold.minute') }}
    {% endif %}
