{{ config(
    schema = 'uniswap_v3_optimism',
    alias = 'pools_array',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['pool'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.creation_block_time')]
    )
}}

SELECT 
    'optimism' AS blockchain
    , 'uniswap' AS project
    , '3' AS version
    , f.pool as pool
    , f.fee as fee 
    , array_agg(
        CAST(ROW(f.token0, f.token1) as ROW(token0 VARBINARY, token1 VARBINARY))
    ) AS tokens
    , array_agg(
        CAST(ROW(erc20a.symbol, erc20b.symbol) as ROW(token0symbol VARCHAR, token1symbol VARCHAR))
    ) AS token_symbols
    , 2 AS tokens_in_pool
    , f.creation_block_time
    , f.creation_block_number
    , f.contract_address
FROM 
{{ ref('uniswap_optimism_pools') }} f
LEFT JOIN 
{{ source('tokens', 'erc20') }} erc20a 
    ON f.token0 = erc20a.contract_address
    AND erc20a.blockchain = 'optimism'
LEFT JOIN 
{{ source('tokens', 'erc20') }} erc20b
    ON f.token1 = erc20b.contract_address
    AND erc20b.blockchain = 'optimism'
{% if is_incremental() %}
WHERE {{ incremental_predicate('f.creation_block_time') }}
{% endif %}
GROUP BY 1, 2, 3, 4, 5, 8, 9, 10, 11