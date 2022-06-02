{{config(schema = 'uniswap_v3', 
        alias='trades',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_id')
}}
        
SELECT
    tx_hash || evt_index::string as unique_id,
    'ethereum' as blockchain,
    'uniswap' as project, 
    'v3' as version,
    dex.block_time,
    erc20a.symbol AS token_a_symbol,
    erc20b.symbol AS token_b_symbol,
    token_a_amount_raw / power(10, erc20a.decimals) AS token_a_amount,
    token_b_amount_raw / power(10, erc20b.decimals) AS token_b_amount,
    coalesce(trader_a, tx.from) as trader_a, -- subqueries rely on this COALESCE to avoid redundant joins with the transactions table
    trader_b,
    coalesce(
        usd_amount,
        token_a_amount_raw / power(10, erc20a.decimals) * pa.price,
        token_b_amount_raw / power(10, erc20b.decimals) * pb.price
        ) as usd_amount,
    token_a_address,
    token_b_address,
    exchange_contract_address,
    tx_hash,
    tx.from as tx_from,
    tx.to as tx_to,
    evt_index as trade_id
    FROM (--Uniswap v3
    SELECT
    t.evt_block_time AS block_time,
    t.recipient AS trader_a,
    cast(NULL as string) AS trader_b,
        -- when amount0 is negative it means trader_a is buying token0 from the pool
        CASE WHEN amount0 < 0 THEN abs(amount0) ELSE abs(amount1) END AS token_a_amount_raw,
        CASE WHEN amount0 < 0 THEN abs(amount1) ELSE abs(amount0) END AS token_b_amount_raw,
        NULL::double AS usd_amount,
        CASE WHEN amount0 < 0 THEN f.token0 ELSE f.token1 END AS token_a_address,
        CASE WHEN amount0 < 0 THEN f.token1 ELSE f.token0 END AS token_b_address,
        t.contract_address as exchange_contract_address,
        t.evt_tx_hash AS tx_hash,
        t.evt_index
        FROM {{ source('uniswap_v3_ethereum', 'pair_evt_swap') }} t
        INNER JOIN {{ source('uniswap_v3_ethereum', 'factory_evt_poolcreated') }} f ON t.evt_tx_hash = f.evt_tx_hash
        ) dex
    INNER JOIN {{ source('ethereum', 'transactions') }} tx
    ON dex.tx_hash = tx.hash
    LEFT JOIN {{ ref('tokens_ethereum_erc20') }} erc20a ON erc20a.contract_address = dex.token_a_address
    LEFT JOIN {{ ref('tokens_ethereum_erc20') }} erc20b ON erc20b.contract_address = dex.token_b_address
    LEFT JOIN {{ source('prices', 'usd') }} pa ON pa.minute = date_trunc('minute', dex.block_time)
        AND pa.contract_address = dex.token_a_address
        AND pa.blockchain = 'ethereum'
    LEFT JOIN {{ source('prices', 'usd') }} pb ON pb.minute = date_trunc('minute', dex.block_time)
        AND pb.contract_address = dex.token_b_address
        AND pb.blockchain = 'ethereum'
{% if is_incremental() %}
-- this filter will only be applied on an incremental run
WHERE dex.block_time > now() - interval 2 days
{% endif %} 