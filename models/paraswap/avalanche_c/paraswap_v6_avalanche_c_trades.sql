{{ config(
    schema = 'paraswap_v6_avalanche_c',
    alias = 'trades',    
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'method', 'trace_address'],
    post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                "project",
                                "paraswap_v6",
                                \'["eptighte", "mwamedacen"]\') }}'
    )
}}

{% set project_start_date = '2024-03-01' %}

with dexs AS (
    SELECT 
        blockTime AS block_time,
        blockNumber AS block_number,
        from_hex(beneficiary) AS taker, 
        null AS maker,  -- TODO: can parse from traces
        receivedAmount AS token_bought_amount_raw,
        fromAmount AS token_sold_amount_raw,
        CAST(NULL AS double) AS amount_usd,  
        method,              
        CASE 
            WHEN from_hex(destToken) = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
            THEN 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7 -- wavax 
            ELSE from_hex(destToken)
        END AS token_bought_address,        
        CASE 
            WHEN from_hex(srcToken) = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
            THEN 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7 -- wavax 
            ELSE from_hex(srcToken)
        END AS token_sold_address,
        projectContractAddress as project_contract_address,
        txHash AS tx_hash, 
        callTraceAddress AS trace_address,
        CAST(-1 as integer) AS evt_index
    FROM {{ ref('paraswap_v6_avalanche_c_trades_decoded') }}     
    {% if is_incremental() %}
    WHERE blockTime >= date_trunc('day', now() - interval '7' day)
    {% endif %}
),

-- USDC.e AND USDT.e price are missing before 2022-10
price_missed_previous AS (
    WITH usdc_price AS (
        SELECT minute, contract_address, decimals, symbol, price
        FROM {{ source('prices', 'usd') }}
        WHERE contract_address = 0xa7d7079b0fead91f3e65f86e8915cb59c1a4c664 -- USDC.e
        ORDER BY minute
        LIMIT 1
    ),

     usdt_price AS (
        SELECT minute, contract_address, decimals, symbol, price
        FROM {{ source('prices', 'usd') }}
        WHERE contract_address = 0xc7198437980c041c805a1edcba50c1ce5db95118 -- USDT.e
        ORDER BY minute
        LIMIT 1
    )

    SELECT minute, contract_address, decimals, symbol, price
    FROM usdc_price
        UNION ALL

    SELECT minute, contract_address, decimals, symbol, price
    FROM usdt_price
),
--  USDC.e AND USDT.e price may be missed for latest swaps
price_missed_next AS (
    WITH usdc_price AS (
        SELECT minute, contract_address, decimals, symbol, price
        FROM {{ source('prices', 'usd') }}
        WHERE contract_address = 0xa7d7079b0fead91f3e65f86e8915cb59c1a4c664 -- USDC.e
        ORDER BY minute DESC
        LIMIT 1
    ),

    usdt_price AS (
    SELECT minute, contract_address, decimals, symbol, price
    FROM {{ source('prices', 'usd') }}
    WHERE contract_address = 0xc7198437980c041c805a1edcba50c1ce5db95118 -- USDT.e
    ORDER BY minute DESC
    LIMIT 1
)

    SELECT minute, contract_address, decimals, symbol, price
    FROM usdc_price

    UNION ALL
    
    SELECT minute, contract_address, decimals, symbol, price
    FROM usdt_price
)

SELECT 'avalanche_c' AS blockchain,
    'paraswap' AS project,
    '6' AS version,
    cast(date_trunc('day', d.block_time) as date) as block_date,
    cast(date_trunc('month', d.block_time) as date) as block_month,    
    d.block_time,
    method,
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
    coalesce(d.maker, tx."from") as maker,
    d.project_contract_address,
    d.tx_hash,
    tx."from" AS tx_from,
    tx.to AS tx_to,
    d.trace_address,
    d.evt_index
FROM dexs d
INNER JOIN {{ source('avalanche_c', 'transactions') }} tx ON d.tx_hash = tx.hash
    AND d.block_number = tx.block_number
    {% if not is_incremental() %}
    AND tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} e1 ON e1.contract_address = d.token_bought_address
    AND e1.blockchain = 'avalanche_c'
LEFT JOIN {{ source('tokens', 'erc20') }} e2 on e2.contract_address = d.token_sold_address
    AND e2.blockchain = 'avalanche_c'
LEFT JOIN {{ source('prices', 'usd') }} p1 ON p1.minute = date_trunc('minute', d.block_time)
    AND p1.contract_address = d.token_bought_address
    AND p1.blockchain = 'avalanche_c'
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
    AND p2.blockchain = 'avalanche_c'
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
