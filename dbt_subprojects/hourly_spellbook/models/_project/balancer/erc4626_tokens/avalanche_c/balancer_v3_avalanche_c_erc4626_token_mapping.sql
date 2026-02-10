{{
    config(
        schema = 'balancer_v3_avalanche_c',
        alias = 'erc4626_token_mapping', 
        materialized = 'table',
        file_format = 'delta'
    )
}}

WITH aave_tokens AS (
    -- No Aave ERC4626 tokens are currently modeled on Avalanche C.
    -- This CTE is kept for schema compatibility and returns no rows.
    SELECT
        CAST(NULL AS VARBINARY) AS erc4626_token,
        CAST(NULL AS VARCHAR) AS erc4626_token_name,
        CAST(NULL AS VARCHAR) AS erc4626_token_symbol,
        CAST(NULL AS VARBINARY) AS underlying_token,
        CAST(NULL AS VARCHAR) AS underlying_token_symbol,
        CAST(NULL AS INTEGER) AS decimals
    WHERE 1 = 0
)

SELECT 
    'avalanche_c' AS blockchain, 
    * 
FROM aave_tokens