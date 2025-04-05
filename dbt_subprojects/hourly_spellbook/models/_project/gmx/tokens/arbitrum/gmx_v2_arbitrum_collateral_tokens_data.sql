{{
  config(
    schema = 'gmx_v2_arbitrum',
    alias = 'collateral_tokens_data',    
    materialized = 'view'
    )
}}

SELECT 
    contract_address AS collateral_token, 
    decimals AS collateral_token_decimals
FROM 
    {{ ref('gmx_v2_arbitrum_erc20') }}
