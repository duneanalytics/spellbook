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
    {{ source("ai_data_master", "result_token_information_from_arbitrum_api", database="dune") }}
