{{
  config(
    schema = 'gmx_v2_arbitrum',
    alias = 'markets_data',    
    materialized = 'view'
    )
}}

SELECT
    MCE.market_token AS market,
    ERC20_IT.decimals AS index_token_decimals,
    ERC20_LT.decimals AS long_token_decimals,
    ERC20_ST.decimals AS short_token_decimals  
FROM {{ ref('gmx_v2_arbitrum_market_created') }} AS MCE
LEFT JOIN {{ ref('gmx_v2_arbitrum_erc20') }} AS ERC20_IT
    ON TRY_CAST(ERC20_IT.contract_address AS VARCHAR)
LEFT JOIN {{ ref('gmx_v2_arbitrum_erc20') }} AS ERC20_LT
    ON TRY_CAST(ERC20_LT.contract_address AS VARCHAR) = MCE.long_token 
LEFT JOIN {{ ref('gmx_v2_arbitrum_erc20') }} AS ERC20_ST
    ON TRY_CAST(ERC20_ST.contract_address AS VARCHAR) = MCE.short_token