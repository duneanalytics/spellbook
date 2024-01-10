{{ config(
    
    schema = 'synthetix_optimism',
    alias = 'spot_trades',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index'],
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "synthetix",
                                \'["msilb7"]\') }}'
    )
}}
-- OVM1 launch 2021-11-11
{% set project_start_date = '2021-11-10' %}

WITH dexs AS
(
    -- Migrated from: https://github.com/duneanalytics/dune-v1-abstractions/blob/main/deprecated-dune-v1-abstractions/optimism2/dex/insert_kwenta.sql
    SELECT
         t.evt_block_time AS block_time
        , t.evt_block_number
        ,'toAddress' AS taker
        ,CAST(NULL as VARBINARY) as maker
        ,t.toAmount AS token_bought_amount_raw
        ,t.fromAmount AS token_sold_amount_raw
        ,cast(NULL as double) AS amount_usd
        ,CAST(NULL as VARBINARY) AS token_bought_address
        ,CAST(NULL as VARBINARY) AS token_sold_address
        -- we need to map token symbols. This is not ideal.
        ,from_utf8(bytearray_rtrim(toCurrencyKey)) AS token_bought_symbol
        ,from_utf8(bytearray_rtrim(fromCurrencyKey)) AS token_sold_symbol

        ,t.contract_address as project_contract_address
        ,t.evt_tx_hash AS tx_hash
        ,t.evt_index
    FROM
        {{ source('synthetix_optimism', 'SNX_evt_SynthExchange') }} t
    {% if is_incremental() %}
    -- making this incremental length longer sicne there's manual token list updates needed
    WHERE t.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
)
SELECT
     'optimism' AS blockchain
    ,'synthetix' AS project
    ,'1' AS version
    ,cast(date_trunc('month', dexs.block_time) AS date) AS block_month
    ,cast(date_trunc('DAY', dexs.block_time) AS date) AS block_date
    ,dexs.block_time
    ,erc20a.symbol AS token_bought_symbol
    ,erc20b.symbol AS token_sold_symbol
    ,case
        when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
        else concat(erc20a.symbol, '-', erc20b.symbol)
    end as token_pair
    ,dexs.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount
    ,dexs.token_sold_amount_raw / power(10, erc20b.decimals) AS token_sold_amount
    ,dexs.token_bought_amount_raw AS token_bought_amount_raw
    ,dexs.token_sold_amount_raw AS token_sold_amount_raw
    ,coalesce(
        dexs.amount_usd
        ,(dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
        ,(dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
    ) AS amount_usd
    ,erc20a.contract_address AS token_bought_address
    ,erc20b.contract_address AS token_sold_address
    ,tx."from" AS taker -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    ,dexs.maker
    ,dexs.project_contract_address
    ,dexs.tx_hash
    ,tx."from" AS tx_from
    ,tx.to AS tx_to
    ,dexs.evt_index
FROM dexs
INNER JOIN {{ source('optimism', 'transactions') }} tx
    ON tx.hash = dexs.tx_hash
    AND tx.block_number = dexs.evt_block_number
    {% if not is_incremental() %}
    AND tx.block_time >= timestamp '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} erc20a
    ON erc20a.symbol = dexs.token_bought_symbol
    AND erc20a.blockchain = 'optimism'
LEFT JOIN {{ source('tokens', 'erc20') }} erc20b
    ON erc20b.symbol = dexs.token_sold_symbol
    AND erc20b.blockchain = 'optimism'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND p_bought.contract_address = erc20a.contract_address
    AND p_bought.blockchain = 'optimism'
    {% if not is_incremental() %}
    AND p_bought.minute >= timestamp '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = erc20b.contract_address
    AND p_sold.blockchain = 'optimism'
    {% if not is_incremental() %}
    AND p_sold.minute >= timestamp '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
