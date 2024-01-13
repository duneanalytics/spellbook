{{ config(
    schema = 'paraswap_v5_avalanche_c',
    alias = 'trades',
    
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
    post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                "project",
                                "paraswap_v5",
                                \'["Henrystats"]\') }}'
    )
}}

{% set project_start_date = '2021-09-08' %} -- min(evt_block_time) in bought & swapped events

WITH 

{% set trade_event_tables = [
    source('paraswap_avalanche_c', 'AugustusSwapperV5_evt_Bought')
    ,source('paraswap_avalanche_c', 'AugustusSwapperV5_evt_Swapped')
    ,source('paraswap_avalanche_c', 'AugustusSwapperV5_evt_Bought2')
    ,source('paraswap_avalanche_c', 'AugustusSwapperV5_evt_Swapped2')
    ,source('paraswap_avalanche_c', 'AugustusSwapperV5_evt_BoughtV3')
    ,source('paraswap_avalanche_c', 'AugustusSwapperV5_evt_SwappedV3')
    ,source('paraswap_avalanche_c', 'AugustusSwapperV5_evt_SwappedDirect')
] %}

dexs as (
    {% for trade_tables in trade_event_tables %}
        SELECT 
            evt_block_time as block_time,
            evt_block_number as block_number,
            beneficiary as taker, 
            initiator as maker, 
            receivedAmount as token_bought_amount_raw,
            srcAmount as token_sold_amount_raw,
            CAST(NULL as double) as amount_usd,
            CASE 
                WHEN destToken = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
                THEN 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7 -- wavax
                ELSE destToken
            END as token_bought_address,
            CASE 
                WHEN srcToken = 0xeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeeee
                THEN 0xb31f66aa3c1e785363f0875a1b74e27b85fd66c7 -- wavax
                ELSE srcToken
            END as token_sold_address,
            contract_address as project_contract_address,
            evt_tx_hash as tx_hash, 
            CAST(ARRAY[-1] as array<bigint>) AS trace_address,
            evt_index
        FROM {{ trade_tables }} p 
        {% if is_incremental() %}
        WHERE p.evt_block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
    {% endfor %}
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

SELECT
    'avalanche_c' as blockchain,
    'paraswap' as project,
    '5' as version,
    cast(date_trunc('day', dexs.block_time) as date) as block_date,
    cast(date_trunc('month', dexs.block_time) as date) as block_month,
    dexs.block_time,
    erc20a.symbol as token_bought_symbol,
    erc20b.symbol as token_sold_symbol,
    case
        when lower(erc20a.symbol) > lower(erc20b.symbol) then concat(erc20b.symbol, '-', erc20a.symbol)
        else concat(erc20a.symbol, '-', erc20b.symbol)
    end as token_pair,
    dexs.token_bought_amount_raw / power(10, erc20a.decimals) AS token_bought_amount,
    dexs.token_sold_amount_raw / power(10, erc20b.decimals) AS token_sold_amount,
    dexs.token_bought_amount_raw,
    dexs.token_sold_amount_raw,
    coalesce(
        dexs.amount_usd
        ,(dexs.token_bought_amount_raw / power(10, erc20a.decimals)) * coalesce(p_bought.price, p_prev1.price, p_next1.price)
        ,(dexs.token_sold_amount_raw / power(10, erc20b.decimals)) * coalesce(p_sold.price, p_prev2.price, p_next2.price)
    ) AS amount_usd,
    dexs.token_bought_address,
    dexs.token_sold_address,
    coalesce(dexs.taker, tx."from") AS taker,
    dexs.maker,
    dexs.project_contract_address,
    dexs.tx_hash,
    tx."from" AS tx_from,
    tx.to AS tx_to,
    dexs.trace_address,
    dexs.evt_index
FROM dexs
INNER JOIN {{ source('avalanche_c', 'transactions') }} tx
    ON dexs.tx_hash = tx.hash
    AND dexs.block_number = tx.block_number
    {% if not is_incremental() %}
    AND tx.block_time >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND tx.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN {{ source('tokens', 'erc20') }} erc20a
    ON erc20a.contract_address = dexs.token_bought_address
    AND erc20a.blockchain = 'avalanche_c'
LEFT JOIN {{ source('tokens', 'erc20') }} erc20b
    ON erc20b.contract_address = dexs.token_sold_address
    AND erc20b.blockchain = 'avalanche_c'
LEFT JOIN {{ source('prices', 'usd') }} p_bought
    ON p_bought.minute = date_trunc('minute', dexs.block_time)
    AND p_bought.contract_address = dexs.token_bought_address
    AND p_bought.blockchain = 'avalanche_c'
    {% if not is_incremental() %}
    AND p_bought.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_bought.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN price_missed_previous p_prev1 ON dexs.token_bought_address = p_prev1.contract_address
    AND dexs.block_time < p_prev1.minute -- Swap before first price record time
LEFT JOIN price_missed_next p_next1 ON dexs.token_bought_address = p_next1.contract_address
    AND dexs.block_time > p_next1.minute -- Swap after last price record time
LEFT JOIN {{ source('prices', 'usd') }} p_sold
    ON p_sold.minute = date_trunc('minute', dexs.block_time)
    AND p_sold.contract_address = dexs.token_sold_address
    AND p_sold.blockchain = 'avalanche_c'
    {% if not is_incremental() %}
    AND p_sold.minute >= TIMESTAMP '{{project_start_date}}'
    {% endif %}
    {% if is_incremental() %}
    AND p_sold.minute >= date_trunc('day', now() - interval '7' day)
    {% endif %}
LEFT JOIN price_missed_previous p_prev2 ON dexs.token_sold_address = p_prev2.contract_address
    AND dexs.block_time < p_prev2.minute -- Swap before first price record time
LEFT JOIN price_missed_next p_next2 ON dexs.token_sold_address = p_next2.contract_address
    AND dexs.block_time > p_next2.minute -- Swap after last price record time
