{{
  config(
    schema = 'gmx_v2_avalanche_c',
    alias = 'markets_data',    
    materialized = 'view'
    )
}}

SELECT
    MCE.market_token AS market,
    CONCAT(ERC20_IT.index_token, '/USD [', ERC20_LT.long_token, '-', ERC20_ST.short_token, ']') AS market_name,
    'GM' AS market_token_symbol,
    18 AS market_token_decimals,
    ERC20_IT.index_token AS index_token_symbol,
    ERC20_IT.decimals AS index_token_decimals,
    ERC20_LT.long_token AS long_token_symbol,
    ERC20_LT.decimals AS long_token_decimals,
    ERC20_ST.short_token AS short_token_symbol,
    ERC20_ST.decimals AS short_token_decimals 
FROM {{ ref('gmx_v2_avalanche_c_market_created') }} AS MCE
LEFT JOIN {{ ref('gmx_v2_avalanche_c_erc20') }} AS ERC20_IT
    ON ERC20_IT.contract_address = MCE.index_token
LEFT JOIN {{ ref('gmx_v2_avalanche_c_erc20') }} AS ERC20_LT
    ON ERC20_LT.contract_address = MCE.long_token 
LEFT JOIN {{ ref('gmx_v2_avalanche_c_erc20') }} AS ERC20_ST
    ON ERC20_ST.contract_address = MCE.short_token