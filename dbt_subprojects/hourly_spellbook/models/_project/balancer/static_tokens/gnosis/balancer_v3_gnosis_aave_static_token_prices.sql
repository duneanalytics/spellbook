{{ config(
        schema = 'balancer_v3_gnosis',
        alias = 'aave_static_token_prices',
        materialized = 'table',
        file_format = 'delta'
    )
}}

WITH wrap_unwrap AS(
        SELECT 
            evt_block_time,
            underlyingToken, 
            wrappedToken,
            CAST(mintedShares AS DOUBLE) / CAST(depositedUnderlying AS DOUBLE) AS ratio
        FROM {{ source('balancer_v3_gnosis', 'Vault_evt_Wrap') }}

        UNION ALL

        SELECT 
            evt_block_time,
            underlyingToken,
            wrappedToken, 
            CAST(burnedShares AS DOUBLE) / CAST(withdrawnUnderlying AS DOUBLE) AS ratio
        FROM {{ source('balancer_v3_gnosis', 'Vault_evt_Unwrap') }}    
    ),


    price_join AS(
    SELECT 
        w.evt_block_time,
        w.underlyingToken,
        w.wrappedToken,
        p.decimals,
        ratio * price AS adjusted_price
    FROM wrap_unwrap w
    JOIN {{ source('prices', 'usd') }} p w.underlyingToken = p.contract_address
    AND p.blockchain = 'gnosis'
    AND DATE_TRUNC('minute', w.evt_block_time) = DATE_TRUNC('minute', p.minute)
    )

SELECT
    DATE_TRUNC('minute', p.evt_block_time) AS minute,
    p.wrappedToken AS wrapped_token,
    p.underlyingToken AS underlying_token,
    m.staticATokenSymbol AS static_atoken_symbol,
    m.underlyingTokenSymbol AS underlying_token_symbol,
    p.decimals AS decimals,
    APPROX_PERCENTILE(adjusted_price, 0.5) AS median_price
FROM price_join p
JOIN {{ref('balancer_v3_gnosis_aave_static_token_mapping')}} m ON m.underlying_token = p.underlyingToken
GROUP BY 1, 2, 3, 4, 5