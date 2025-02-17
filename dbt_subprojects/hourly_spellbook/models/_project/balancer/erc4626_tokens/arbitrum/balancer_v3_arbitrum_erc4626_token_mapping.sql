{{
    config(
        schema = 'balancer_v3_arbitrum',
        alias = 'erc4626_token_mapping', 
        materialized = 'table',
        file_format = 'delta'
    )
}}

WITH aave_tokens AS(
SELECT 
    b.output_0 AS erc4626_token,
    t2.symbol AS erc4626_token_name,
    t2.symbol AS erc4626_token_symbol,
    BYTEARRAY_SUBSTRING(b.salt, 13, 24) AS underlying_token,
    t1.symbol AS underlying_token_symbol,
    t2.decimals AS decimals
FROM {{ source('aave_v3_arbitrum', 'StataToken_evt_Initialized') }} a
JOIN {{ source('aave_v3_arbitrum', 'StataTokenFactory_call_createDeterministic') }} b
ON b.output_0 = a.contract_address
JOIN {{ source('tokens', 'erc20') }} t1
ON t1.blockchain = 'arbitrum'
AND BYTEARRAY_SUBSTRING(b.salt, 13, 36) = t1.contract_address
JOIN {{ source('tokens', 'erc20') }} t2
ON t2.blockchain = 'arbitrum'
AND b.output_0 = t2.contract_address)

SELECT 
    'arbitrum' AS blockchain, 
    * 
FROM aave_tokens