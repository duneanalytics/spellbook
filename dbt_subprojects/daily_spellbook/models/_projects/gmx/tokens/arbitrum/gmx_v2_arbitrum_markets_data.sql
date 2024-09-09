{{
  config(
    schema = 'gmx_v2_arbitrum',
    alias = 'markets_data',    
    materialized = 'view'
    )
}}

SELECT
    MCE.market_token AS market,
    CONCAT(ERC20_IT.symbol, '/USD [', ERC20_LT.symbol, '-', ERC20_ST.symbol, ']') AS market_name,
    'GM' AS market_token_symbol,
    18 AS market_token_decimals,
    ERC20_IT.symbol AS index_token_symbol,
    ERC20_IT.decimals AS index_token_decimals,
    ERC20_LT.symbol AS long_token_symbol,
    ERC20_LT.decimals AS long_token_decimals,
    ERC20_ST.symbol AS short_token_symbol,
    ERC20_ST.decimals AS short_token_decimals  
FROM {{ ref('gmx_v2_arbitrum_market_created') }} AS MCE
LEFT JOIN {{ ref('gmx_v2_arbitrum_erc20') }} AS ERC20_IT
    ON ERC20_IT.contract_address = MCE.index_token
LEFT JOIN {{ ref('gmx_v2_arbitrum_erc20') }} AS ERC20_LT
    ON ERC20_LT.contract_address = MCE.long_token 
LEFT JOIN {{ ref('gmx_v2_arbitrum_erc20') }} AS ERC20_ST
    ON ERC20_ST.contract_address = MCE.short_token