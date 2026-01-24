{{
    config(
        schema = 'balancer_v3_hyperevm',
        alias = 'erc4626_token_mapping', 
        materialized = 'table',
        file_format = 'delta'
    )
}}

WITH aave_tokens AS (
    SELECT 
        b.output_0[1] AS erc4626_token,
        t2.symbol AS erc4626_token_name,
        t2.symbol AS erc4626_token_symbol,
        b.underlyings[1] AS underlying_token,
        t1.symbol AS underlying_token_symbol,
        t2.decimals AS decimals
    FROM {{ source('aave_v3_hyperevm', 'StataTokenV2_evt_Initialized') }} a
    JOIN {{ source('aave_v3_hyperevm', 'StataTokenFactory_call_createstatatokens') }} b
        ON b.output_0[1] = a.contract_address
    JOIN {{ source('tokens', 'erc20') }} t1
        ON t1.blockchain = 'hyperevm'
        AND b.underlyings[1] = t1.contract_address
    JOIN {{ source('tokens', 'erc20') }} t2
        ON t2.blockchain = 'hyperevm'
        AND b.output_0[1] = t2.contract_address
)

SELECT 
    'hyperevm' AS blockchain, 
    * 
FROM aave_tokens