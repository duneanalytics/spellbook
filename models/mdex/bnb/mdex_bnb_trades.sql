{{ config(
    
    alias = 'trades'
    ,partition_by = ['block_month']
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy = 'merge'
    ,unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    ,post_hook='{{ expose_spells(\'["bnb"]\',
                                      "project",
                                      "mdex",
                                    \'["codingsh"]\') }}'
    )
}}

{% set project_start_date = '2021-04-05' %}

WITH mdex_dex AS (
    SELECT  t.evt_block_time                                                       AS block_time,
            "to"                                                                   AS taker,
            sender                                                                 AS maker,
            CASE WHEN amount0Out = UINT256 '0' THEN amount1Out ELSE amount0Out END AS token_bought_amount_raw,
            CASE WHEN amount0In = UINT256 '0' THEN amount1In ELSE amount0In END    AS token_sold_amount_raw,
            NULL                                                                   AS amount_usd,
            CASE WHEN amount0Out = UINT256 '0' THEN token1 ELSE token0 END         AS token_bought_address,
            CASE WHEN amount0In = UINT256 '0' THEN token1 ELSE token0 END          AS token_sold_address,
            t.contract_address                                                     AS project_contract_address,
            t.evt_tx_hash                                                          AS tx_hash,
            t.evt_index
    FROM {{ source('mdex_bnb', 'MdexPair_evt_Swap') }} t
    INNER JOIN {{ source('mdex_bnb', 'MdexFactory_evt_PairCreated') }} f
        ON f.pair = t.contract_address
    {% if is_incremental() %}
    WHERE t.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    {% if not is_incremental() %}
    WHERE t.evt_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
)

SELECT
    'bnb'                                                         AS blockchain,
    'mdex'                                                        AS project,
    '1'                                                           AS version,
    cast(date_trunc('DAY', mdex_dex.block_time) AS date)          AS block_date,
    cast(date_trunc('month', mdex_dex.block_time) AS date)        AS block_month,
    mdex_dex.block_time,
    erc20a.symbol                                                 AS token_bought_symbol,
    erc20b.symbol                                                 AS token_sold_symbol,
    CASE
        WHEN lower(erc20a.symbol) > lower(erc20b.symbol) THEN concat(erc20b.symbol, '-', erc20a.symbol)
        ELSE concat(erc20a.symbol, '-', erc20b.symbol)
        END                                                       AS token_pair,
    mdex_dex.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount,
    mdex_dex.token_sold_amount_raw / power(10, erc20b.decimals)   AS token_sold_amount,
    mdex_dex.token_bought_amount_raw AS token_bought_amount_raw,
    mdex_dex.token_sold_amount_raw AS token_sold_amount_raw,
    coalesce(
            mdex_dex.amount_usd
        , (mdex_dex.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
        , (mdex_dex.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
        )                                                         AS amount_usd,
    mdex_dex.token_bought_address,
    mdex_dex.token_sold_address,
    coalesce(mdex_dex.taker, tx."from")                           AS taker,
    mdex_dex.maker,
    mdex_dex.project_contract_address,
    mdex_dex.tx_hash,
    tx."from"                                                     AS tx_from,
    tx.to                                                         AS tx_to,
    mdex_dex.evt_index
FROM mdex_dex
INNER JOIN {{ source('bnb', 'transactions') }} tx
    ON mdex_dex.tx_hash = tx.hash
    {% if not is_incremental() %}
    AND tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} erc20a
    ON erc20a.contract_address = mdex_dex.token_bought_address
    AND erc20a.blockchain = 'bnb'
LEFT JOIN {{ source('tokens', 'erc20') }} erc20b
    ON erc20b.contract_address = mdex_dex.token_sold_address
    AND erc20b.blockchain = 'bnb'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', mdex_dex.block_time)
    AND p_bought.contract_address = mdex_dex.token_bought_address
    AND p_bought.blockchain = 'bnb'
    {% if not is_incremental() %}
    AND p_bought.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', mdex_dex.block_time)
    AND p_sold.contract_address = mdex_dex.token_sold_address
    AND p_sold.blockchain = 'bnb'
    {% if not is_incremental() %}
    AND p_sold.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    