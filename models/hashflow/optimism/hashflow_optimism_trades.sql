{{ config(
    alias = 'trades'
    ,schema = 'hashflow_optimism'
    ,partition_by = ['block_month']
    ,materialized = 'incremental'
    ,file_format = 'delta'
    ,incremental_strategy = 'merge'
    ,unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    ,post_hook='{{ expose_spells(\'["optimism"]\',
                                      "project",
                                      "hashflow",
                                    \'["ARDev097"]\') }}'
    )
}}

{% set project_start_date = '2022-05-03' %}
{% set hashflow_optimism_evt_trade_tables = [
    source('hashflow_optimism', 'Pool_evt_Trade')
    , source('hashflow_optimism', 'Pool_evt_LzTrade')
    , source('hashflow_optimism', 'Pool_evt_XChainTrade')
] %}


with dexs AS (
    {% for evt_trade_table in hashflow_optimism_evt_trade_tables %}
        SELECT
            evt_block_time          AS block_time,
            trader                  AS taker,
            CAST(NULL as VARBINARY) AS maker,
            quoteTokenAmount        AS token_bought_amount_raw,
            baseTokenAmount         AS token_sold_amount_raw,
            CAST(NULL AS double)    AS amount_usd,
            quoteToken              AS token_bought_address,
            baseToken               AS token_sold_address,
            contract_address        AS project_contract_address,
            evt_tx_hash             AS tx_hash,
            evt_index
        FROM {{ evt_trade_table }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% else %}
        WHERE evt_block_time >= TIMESTAMP '{{project_start_date}}'
        {% endif %}

        {% if not loop.last %}
        UNION ALL
        {% endif %}

    {% endfor %}
)

SELECT
    'optimism'                                             AS blockchain,
    'hashflow'                                                AS project,
    '1'                                                       AS version,
    CAST(date_trunc('DAY', dexs.block_time) AS date)          AS block_date,
    CAST(date_trunc('MONTH', dexs.block_time) AS date)        AS block_month,
    dexs.block_time,
    erc20a.symbol                                             AS token_bought_symbol,
    erc20b.symbol                                             AS token_sold_symbol,
    CASE
        WHEN lower(erc20a.symbol) > lower(erc20b.symbol)
        THEN concat(erc20b.symbol, '-', erc20a.symbol)
        ELSE concat(erc20a.symbol, '-', erc20b.symbol)
        END                                                   AS token_pair,
    dexs.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount,
    dexs.token_sold_amount_raw / power(10, erc20b.decimals)   AS token_sold_amount,
    dexs.token_bought_amount_raw                              AS token_bought_amount_raw,
    dexs.token_sold_amount_raw                                AS token_sold_amount_raw,
    coalesce(
            dexs.amount_usd
        , (dexs.token_bought_amount_raw / power(10, p_bought.decimals)) * p_bought.price
        , (dexs.token_sold_amount_raw / power(10, p_sold.decimals)) * p_sold.price
        )                                                     AS amount_usd,
    dexs.token_bought_address,
    dexs.token_sold_address,
    coalesce(dexs.taker, tx."from")                           AS taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    tx."from"                                                 AS tx_from,
    tx.to                                                     AS tx_to,
    dexs.evt_index
FROM dexs
INNER JOIN {{ source('optimism', 'transactions') }} tx
    ON dexs.tx_hash = tx.hash
    {% if not is_incremental() %}
    AND tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} erc20a
    ON erc20a.contract_address = dexs.token_bought_address
    AND erc20a.blockchain = 'optimism'
LEFT JOIN {{ source('tokens', 'erc20') }} erc20b
    ON erc20b.contract_address = dexs.token_sold_address
    AND erc20b.blockchain = 'optimism'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    AND p_bought.blockchain = 'optimism'
    {% if not is_incremental() %}
    AND p_bought.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = 'optimism'
    {% if not is_incremental() %}
    AND p_sold.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
