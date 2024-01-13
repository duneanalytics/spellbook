{{ config(
    schema = 'paraswap_v4_polygon',
    alias = 'trades',
    
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["polygon"]\',
                                "project",
                                "paraswap_v4",
                                \'["springzh"]\') }}'
    )
}}

{% set project_start_date = '2021-04-12' %}
{% set trade_event_tables = [
    source('paraswap_polygon', 'AugustusSwapperV4_evt_Bought')
    ,source('paraswap_polygon', 'AugustusSwapperV4_evt_Swapped')
] %}

WITH dexs AS (
    {% for trade_table in trade_event_tables %}
        SELECT 
            evt_block_time AS block_time,
            evt_block_number AS block_number,
            beneficiary AS taker, 
            initiator AS maker, 
            receivedAmount AS token_bought_amount_raw,
            srcAmount AS token_sold_amount_raw,
            CAST(NULL AS double) AS amount_usd,
            CASE 
                WHEN destToken = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
                THEN 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270 -- WMATIC 
                ELSE destToken
            END AS token_bought_address,
            CASE 
                WHEN srcToken = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
                THEN 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270 -- WMATIC 
                ELSE srcToken
            END AS token_sold_address,
            contract_address AS project_contract_address,
            evt_tx_hash AS tx_hash, 
            CAST(ARRAY[-1] as array<bigint>) AS trace_address,
            evt_index
        FROM {{ trade_table }} p 
        {% if is_incremental() %}
        WHERE p.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
),

-- WMATIC price are missing before 2022-02-05
price_missed_previous AS (
    SELECT minute, contract_address, decimals, symbol, price
    FROM {{ source('prices', 'usd') }}
    WHERE contract_address = 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270 -- WMATIC
    ORDER BY minute
    LIMIT 1
),

--  WMATIC price may be missed for latest swaps
price_missed_next AS (
    SELECT minute, contract_address, decimals, symbol, price
    FROM {{ source('prices', 'usd') }}
    WHERE contract_address = 0x0d500b1d8e8ef31e21c99d1db9a6444d3adf1270 -- WMATIC
    ORDER BY minute desc
    LIMIT 1
)

SELECT 'polygon' AS blockchain,
    'paraswap' AS project,
    '4' AS version,
    cast(date_trunc('day', d.block_time) as date) as block_date,
    cast(date_trunc('month', d.block_time) as date) as block_month,
    d.block_time,
    e1.symbol AS token_bought_symbol,
    e2.symbol AS token_sold_symbol,
    CASE
        WHEN lower(e1.symbol) > lower(e2.symbol) THEN concat(e2.symbol, '-', e1.symbol)
        ELSE concat(e1.symbol, '-', e2.symbol)
    END AS token_pair,
    d.token_bought_amount_raw / power(10, e1.decimals) AS token_bought_amount,
    d.token_sold_amount_raw / power(10, e2.decimals) AS token_sold_amount,
    d.token_bought_amount_raw,
    d.token_sold_amount_raw,
    coalesce(
        d.amount_usd
        ,(d.token_bought_amount_raw / power(10, e1.decimals)) * coalesce(p1.price, p_prev1.price, p_next1.price)
        ,(d.token_sold_amount_raw / power(10, e2.decimals)) * coalesce(p2.price, p_prev2.price, p_next2.price)
    ) AS amount_usd,
    d.token_bought_address,
    d.token_sold_address,
    coalesce(d.taker, tx."from") AS taker,
    d.maker,
    d.project_contract_address,
    d.tx_hash,
    tx."from" AS tx_from,
    tx.to AS tx_to,
    d.trace_address,
    d.evt_index
FROM dexs d
INNER JOIN {{ source('polygon', 'transactions') }} tx ON d.tx_hash = tx.hash
    AND d.block_number = tx.block_number
    {% if not is_incremental() %}
    AND tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} e1 ON e1.contract_address = d.token_bought_address
    AND e1.blockchain = 'polygon'
LEFT JOIN {{ source('tokens', 'erc20') }} e2 on e2.contract_address = d.token_sold_address
    AND e2.blockchain = 'polygon'
LEFT JOIN {{ source('prices', 'usd') }} p1 ON p1.minute = date_trunc('minute', d.block_time)
    AND p1.contract_address = d.token_bought_address
    AND p1.blockchain = 'polygon'
    {% if not is_incremental() %}
    AND p1.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p1.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN price_missed_previous p_prev1 ON d.token_bought_address = p_prev1.contract_address
    AND d.block_time < p_prev1.minute -- Swap before first price record time
LEFT JOIN price_missed_next p_next1 ON d.token_bought_address = p_next1.contract_address
    AND d.block_time > p_next1.minute -- Swap after last price record time
LEFT JOIN {{ source('prices', 'usd') }} p2 ON p2.minute = date_trunc('minute', d.block_time)
    AND p2.contract_address = d.token_sold_address
    AND p2.blockchain = 'polygon'
    {% if not is_incremental() %}
    AND p2.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p2.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN price_missed_previous p_prev2 ON d.token_sold_address = p_prev2.contract_address
    AND d.block_time < p_prev2.minute -- Swap before first price record time
LEFT JOIN price_missed_next p_next2 ON d.token_sold_address = p_next2.contract_address
    AND d.block_time > p_next2.minute -- Swap after last price record time
