 {{
  config(
        schema = 'uniswap_v2_ethereum', 
        alias='trades',
        materialized ='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key='unique_id'
  )
}}

SELECT
    tx_hash || evt_index::string as unique_id,
    'ethereum' as blockchain,
    'uniswap' as project, 
    'v2' as version,
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
FROM (
    --Uniswap v2
    SELECT
        t.evt_block_time AS block_time,
        t.to AS trader_a,
        cast(NULL as string) AS trader_b,
        -- when amount0 is negative it means trader_a is buying token0 from the pool
        CASE WHEN amount0Out = 0 THEN amount1Out ELSE amount0Out END AS token_a_amount_raw,
        CASE WHEN amount0In = 0 OR amount1Out = 0 THEN amount1In ELSE amount0In END AS token_b_amount_raw,
        NULL::double AS usd_amount,
        CASE WHEN amount0Out = 0 THEN f.token1 ELSE f.token0 END AS token_a_address,
        CASE WHEN amount0In = 0 OR amount1Out = 0 THEN f.token1 ELSE f.token0 END AS token_b_address,
        t.contract_address as exchange_contract_address,
        t.evt_tx_hash AS tx_hash,
        t.evt_index
    FROM {{ source('uniswap_v2_ethereum', 'pair_evt_swap') }} t
    INNER JOIN {{ source('uniswap_v2_ethereum', 'factory_evt_paircreated') }} f ON f.pair = t.contract_address
    WHERE  t.contract_address NOT IN (
            '0xed9c854cb02de75ce4c9bba992828d6cb7fd5c71', -- remove WETH-UBOMB wash trading pair
            '0xf9c1fA7d41bf44ADe1dd08D37CC68f67Ae75bF92', -- remove WETH-WETH wash trading pair 
            '0x854373387e41371ac6e307a1f29603c6fa10d872' ) -- remove FEG/ETH token pair
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