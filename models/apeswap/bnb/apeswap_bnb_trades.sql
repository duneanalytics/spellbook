{{ config(
    
    alias = 'trades'
    ,partition_by = ['block_month']
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy = 'merge'
    ,unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    ,post_hook='{{ expose_spells(\'["bnb"]\',
                                      "project",
                                      "apeswap",
                                    \'["codingsh"]\') }}'
    )
}}

{% set project_start_date = '2021-02-13' %}

WITH apeswap_dex AS (
    SELECT  t.evt_block_time AS block_time,
            t.to AS taker,
            t.sender AS maker,
            CASE WHEN t.amount0Out = UINT256 '0' THEN t.amount1Out ELSE t.amount0Out END AS token_bought_amount_raw,
            CASE WHEN t.amount0In = UINT256 '0' THEN t.amount1In ELSE t.amount0In END AS token_sold_amount_raw,
            cast(NULL as double) AS amount_usd,
            CASE WHEN t.amount0Out = UINT256 '0' THEN f.token1 ELSE f.token0 END AS token_bought_address,
            CASE WHEN t.amount0In = UINT256 '0' THEN f.token1 ELSE f.token0 END AS token_sold_address,
            t.contract_address AS project_contract_address,
            t.evt_tx_hash AS tx_hash,
            t.evt_index
    FROM {{ source('apeswap_bnb', 'ApePair_evt_Swap') }} t
    INNER JOIN {{ source('apeswap_bnb', 'ApeFactory_evt_PairCreated') }} f
        ON f.pair = t.contract_address
    {% if is_incremental() %}
    WHERE t.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    {% if not is_incremental() %}
    WHERE t.evt_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
)

SELECT 'bnb'                                                            AS blockchain,
       'apeswap'                                                        AS project,
       '1'                                                              AS version,
       try_cast(date_trunc('DAY', apeswap_dex.block_time) AS date)      AS block_date,
       cast(date_trunc('month', apeswap_dex.block_time) AS date)        AS block_month,
       apeswap_dex.block_time,
       erc20a.symbol                                                    AS token_bought_symbol,
       erc20b.symbol                                                    AS token_sold_symbol,
       CASE
           WHEN lower(erc20a.symbol) > lower(erc20b.symbol) THEN concat(erc20b.symbol, '-', erc20a.symbol)
           ELSE concat(erc20a.symbol, '-', erc20b.symbol)
           END                                                          AS token_pair,
       apeswap_dex.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount,
       apeswap_dex.token_sold_amount_raw / power(10, erc20b.decimals)   AS token_sold_amount,
       apeswap_dex.token_bought_amount_raw       AS token_bought_amount_raw,
       apeswap_dex.token_sold_amount_raw         AS token_sold_amount_raw,
       coalesce(
               apeswap_dex.amount_usd
           , (apeswap_dex.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
           , (apeswap_dex.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
           )                                                            AS amount_usd,
       apeswap_dex.token_bought_address,
       apeswap_dex.token_sold_address,
       coalesce(apeswap_dex.taker, tx."from")                             AS taker,
       apeswap_dex.maker,
       apeswap_dex.project_contract_address,
       apeswap_dex.tx_hash,
       tx."from"                                                          AS tx_from,
       tx.to                                                            AS tx_to,
       apeswap_dex.evt_index
FROM apeswap_dex
INNER JOIN {{ source('bnb', 'transactions') }} tx
    ON apeswap_dex.tx_hash = tx.hash
    {% if not is_incremental() %}
    AND tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} erc20a
    ON erc20a.contract_address = apeswap_dex.token_bought_address
    AND erc20a.blockchain = 'bnb'
LEFT JOIN {{ source('tokens', 'erc20') }} erc20b
    ON erc20b.contract_address = apeswap_dex.token_sold_address
    AND erc20b.blockchain = 'bnb'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', apeswap_dex.block_time)
    AND p_bought.contract_address = apeswap_dex.token_bought_address
    AND p_bought.blockchain = 'bnb'
    {% if not is_incremental() %}
    AND p_bought.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', apeswap_dex.block_time)
    AND p_sold.contract_address = apeswap_dex.token_sold_address
    AND p_sold.blockchain = 'bnb'
    {% if not is_incremental() %}
    AND p_sold.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    