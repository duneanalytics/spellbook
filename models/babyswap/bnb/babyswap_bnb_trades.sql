{{ config(
    alias = 'trades'
    ,partition_by = ['block_month']
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy = 'merge'
    ,unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    ,post_hook='{{ expose_spells(\'["bnb"]\',
                                      "project",
                                      "babyswap",
                                    \'["codingsh"]\') }}'
    )
}}

{% set project_start_date = '2021-06-01' %}

WITH babyswap_dex AS (
    SELECT  t.evt_block_time                                             AS block_time,
            to                                                           AS taker,
            sender                                                       AS maker,
            CASE WHEN amount0Out = UINT256 '0' THEN amount1Out ELSE amount0Out END AS token_bought_amount_raw,
            CASE WHEN amount0In = UINT256 '0' THEN amount1In ELSE amount0In END    AS token_sold_amount_raw,
            cast(NULL as double)                                         AS amount_usd,
            CASE WHEN amount0Out = UINT256 '0' THEN token1 ELSE token0 END         AS token_bought_address,
            CASE WHEN amount0In = UINT256 '0' THEN token1 ELSE token0 END          AS token_sold_address,
            t.contract_address                                           AS project_contract_address,
            t.evt_tx_hash                                                AS tx_hash,
            ''                                                           AS trace_address,
            t.evt_index
    FROM {{ source('babyswap_bnb', 'BabyPair_evt_Swap') }} t
    INNER JOIN {{ source('babyswap_bnb', 'BabyFactory_evt_PairCreated') }} p
        ON t.contract_address = p.pair
    {% if is_incremental() %}
    WHERE t.evt_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    {% if not is_incremental() %}
    WHERE t.evt_block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
)

SELECT 'bnb'                                                             AS blockchain,
       'babyswap'                                                        AS project,
       '1'                                                               AS version,
       cast(date_trunc('month', babyswap_dex.block_time) AS date)        AS block_month,
       cast(date_trunc('DAY', babyswap_dex.block_time) AS date)          AS block_date,
       babyswap_dex.block_time,
       erc20a.symbol                                                     AS token_bought_symbol,
       erc20b.symbol                                                     AS token_sold_symbol,
       CASE
           WHEN lower(erc20a.symbol) > lower(erc20b.symbol) THEN concat(erc20b.symbol, '-', erc20a.symbol)
           ELSE concat(erc20a.symbol, '-', erc20b.symbol)
           END                                                           AS token_pair,
       babyswap_dex.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount,
       babyswap_dex.token_sold_amount_raw / power(10, erc20b.decimals)   AS token_sold_amount,
       babyswap_dex.token_bought_amount_raw AS token_bought_amount_raw,
       babyswap_dex.token_sold_amount_raw AS token_sold_amount_raw,
       coalesce(
               babyswap_dex.amount_usd
           , (babyswap_dex.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
           , (babyswap_dex.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
           )                                                             AS amount_usd,
       babyswap_dex.token_bought_address,
       babyswap_dex.token_sold_address,
       coalesce(babyswap_dex.taker, tx."from")                             AS taker,
       babyswap_dex.maker,
       babyswap_dex.project_contract_address,
       babyswap_dex.tx_hash,
       tx."from"                                                           AS tx_from,
       tx.to                                                             AS tx_to,
       babyswap_dex.trace_address,
       babyswap_dex.evt_index
FROM babyswap_dex
INNER JOIN {{ source('bnb', 'transactions') }} tx
    ON babyswap_dex.tx_hash = tx.hash
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    {% if not is_incremental() %}
    AND tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} erc20a
    ON erc20a.contract_address = babyswap_dex.token_bought_address
    AND erc20a.blockchain = 'bnb'
LEFT JOIN {{ source('tokens', 'erc20') }} erc20b
    ON erc20b.contract_address = babyswap_dex.token_sold_address
    AND erc20b.blockchain = 'bnb'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', babyswap_dex.block_time)
    AND p_bought.contract_address = babyswap_dex.token_bought_address
    AND p_bought.blockchain = 'bnb'
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    {% if not is_incremental() %}
    AND p_bought.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', babyswap_dex.block_time)
    AND p_sold.contract_address = babyswap_dex.token_sold_address
    AND p_sold.blockchain = 'bnb'
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    {% if not is_incremental() %}
    AND p_sold.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
