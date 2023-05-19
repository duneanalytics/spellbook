{{ config(
    alias = 'trades'
    ,partition_by = ['block_date']
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy = 'merge'
    ,unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    ,post_hook='{{ expose_spells(\'["bnb"]\',
                                      "project",
                                      "knightswap",
                                    \'["seungwookim08"]\') }}'
    )
}}

{% set project_start_date = '2021-11-02' %}


WITH knightswap_dex AS (
    SELECT  t.evt_block_time AS block_time,
            `to` AS taker,
            sender AS maker,
            CASE WHEN amount0Out = 0 THEN amount1Out ELSE amount0Out END AS token_bought_amount_raw,
            CASE WHEN amount0In = 0 THEN amount1In ELSE amount0In END AS token_sold_amount_raw,
            cast(NULL as double) AS amount_usd,
            CASE WHEN amount0Out = 0 THEN token1 ELSE token0 END AS token_bought_address,
            CASE WHEN amount0In = 0 THEN token1 ELSE token0 END AS token_sold_address,
            t.contract_address AS project_contract_address,
            t.evt_tx_hash AS tx_hash,
            '' AS trace_address,
            t.evt_index
    FROM {{ source('knightswap_bnb', 'KnightPair_evt_Swap') }} t
    INNER JOIN {{ source('knightswap_bnb', 'KnightFactory_evt_PairCreated') }} p
        ON t.contract_address = p.pair
    {% if is_incremental() %}
    WHERE t.evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    WHERE t.evt_block_time >= '{{ project_start_date }}'
    {% endif %}
)

SELECT 'bnb' AS blockchain,
       'knightswap' AS project,
       '1' AS version,
       try_cast(date_trunc('DAY', knightswap_dex.block_time) AS date) AS block_date,
       knightswap_dex.block_time,
       erc20a.symbol AS token_bought_symbol,
       erc20b.symbol AS token_sold_symbol,
       CASE
           WHEN lower(erc20a.symbol) > lower(erc20b.symbol) THEN concat(erc20b.symbol, '-', erc20a.symbol)
           ELSE concat(erc20a.symbol, '-', erc20b.symbol)
           END AS token_pair,
       knightswap_dex.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount,
       knightswap_dex.token_sold_amount_raw / power(10, erc20b.decimals) AS token_sold_amount,
       CAST(knightswap_dex.token_bought_amount_raw AS DECIMAL(38,0)) AS token_bought_amount_raw,
       CAST(knightswap_dex.token_sold_amount_raw AS DECIMAL(38,0)) AS token_sold_amount_raw,
       coalesce(
               knightswap_dex.amount_usd
           , (knightswap_dex.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
           , (knightswap_dex.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
           ) AS amount_usd,
       knightswap_dex.token_bought_address,
       knightswap_dex.token_sold_address,
       coalesce(knightswap_dex.taker, tx.from) AS taker,
       knightswap_dex.maker,
       knightswap_dex.project_contract_address,
       knightswap_dex.tx_hash,
       tx.from AS tx_from,
       tx.to AS tx_to,
       knightswap_dex.trace_address,
       knightswap_dex.evt_index
FROM knightswap_dex
INNER JOIN {{ source('bnb', 'transactions') }} tx
    ON knightswap_dex.tx_hash = tx.hash
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    AND tx.block_time >= '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ ref('tokens_bnb_bep20') }} erc20a 
    ON erc20a.contract_address = knightswap_dex.token_bought_address
LEFT JOIN {{ ref('tokens_bnb_bep20') }} erc20b
    ON erc20b.contract_address = knightswap_dex.token_sold_address
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', knightswap_dex.block_time)
    AND p_bought.contract_address = knightswap_dex.token_bought_address
    AND p_bought.blockchain = 'bnb'
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    AND p_bought.minute >= '{{project_start_date}}'
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', knightswap_dex.block_time)
    AND p_sold.contract_address = knightswap_dex.token_sold_address
    AND p_sold.blockchain = 'bnb'
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not is_incremental() %}
    AND p_sold.minute >= '{{project_start_date}}'
    {% endif %}
;