{{ config(
    alias = alias('trades'),
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["bnb"]\',
                                "project",
                                "fraxswap",
                                \'["kndlexi"]\') }}'
    )
}}

{% set project_start_date = '2022-05-23' %}

WITH

fraxswap_dex AS (
    SELECT
        t.evt_block_time                                                    AS block_time
        ,t.`to`                                                             AS taker
        ,''                                                                 AS maker
        ,CASE WHEN t.amount0Out = 0 THEN t.amount1Out ELSE t.amount0Out END AS token_bought_amount_raw
        ,CASE WHEN t.amount0In = 0 THEN t.amount1In ELSE t.amount0In END    AS token_sold_amount_raw
        ,cast(NULL as double)                                               AS amount_usd
        ,CASE WHEN t.amount0Out = 0 THEN p.token1 ELSE p.token0 END         AS token_bought_address
        ,CASE WHEN t.amount0In = 0 THEN p.token1 ELSE p.token0 END          AS token_sold_address
        ,t.contract_address                                                 AS project_contract_address
        ,t.evt_tx_hash                                                      AS tx_hash
        ,''                                                                 AS trace_address
        ,t.evt_index
    FROM {{ source('fraxswap_bnb', 'FraxswapPair_evt_Swap') }} t
    INNER JOIN {{ source('fraxswap_bnb', 'FraxswapFactory_evt_PairCreated') }} p
        ON t.contract_address = p.pair
    {% if is_incremental() %}
    WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    WHERE t.evt_block_time >= '{{ project_start_date }}'
    {% endif %}
)

SELECT
    'bnb'                                                               AS blockchain
    ,'fraxswap'                                                         AS project
    ,'1'                                                                AS version
    ,try_cast(date_trunc('DAY', fraxswap_dex.block_time) AS date)       AS block_date
    ,fraxswap_dex.block_time
    ,erc20a.symbol                                                      AS token_bought_symbol
    ,erc20b.symbol                                                      AS token_sold_symbol
    ,CASE
         WHEN lower(erc20a.symbol) > lower(erc20b.symbol) THEN concat(erc20b.symbol, '-', erc20a.symbol)
         ELSE concat(erc20a.symbol, '-', erc20b.symbol)
     END                                                                AS token_pair
    ,fraxswap_dex.token_bought_amount_raw / power(10, erc20a.decimals)  AS token_bought_amount
    ,fraxswap_dex.token_sold_amount_raw / power(10, erc20b.decimals)    AS token_sold_amount
    ,CAST(fraxswap_dex.token_bought_amount_raw AS DECIMAL(38,0))        AS token_bought_amount_raw
    ,CAST(fraxswap_dex.token_sold_amount_raw AS DECIMAL(38,0))          AS token_sold_amount_raw
    ,coalesce(fraxswap_dex.amount_usd
            ,(fraxswap_dex.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
            ,(fraxswap_dex.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
     )                                                                  AS amount_usd
    ,fraxswap_dex.token_bought_address
    ,fraxswap_dex.token_sold_address
    ,coalesce(fraxswap_dex.taker, tx.from)                              AS taker
    ,fraxswap_dex.maker
    ,fraxswap_dex.project_contract_address
    ,fraxswap_dex.tx_hash
    ,tx.from                                                            AS tx_from
    ,tx.to                                                              AS tx_to
    ,fraxswap_dex.trace_address
    ,fraxswap_dex.evt_index
FROM fraxswap_dex
INNER JOIN {{ source('bnb', 'transactions') }} tx
    ON fraxswap_dex.tx_hash = tx.hash
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    AND tx.block_time >= '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ ref('tokens_erc20') }} erc20a
    ON erc20a.contract_address = fraxswap_dex.token_bought_address
    and erc20a.blockchain = 'bnb'
LEFT JOIN {{ ref('tokens_erc20') }} erc20b
    ON erc20b.contract_address = fraxswap_dex.token_sold_address
    AND erc20b.blockchain = 'bnb'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', fraxswap_dex.block_time)
    AND p_bought.contract_address = fraxswap_dex.token_bought_address
    AND p_bought.blockchain = 'bnb'
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    AND p_bought.minute >= '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', fraxswap_dex.block_time)
    AND p_sold.contract_address = fraxswap_dex.token_sold_address
    AND p_sold.blockchain = 'bnb'
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    AND p_sold.minute >= '{{project_start_date}}'
    {% endif %}
;