{{ config(
        schema = 'balancer_v3_gnosis',
        alias = 'erc4626_token_prices',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['minute', 'wrapped_token'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.minute')]
    )
}}

WITH wrap_unwrap AS(
        SELECT 
            evt_block_time,
            underlyingToken, 
            wrappedToken,
            CAST(depositedUnderlying AS DOUBLE) / CAST(mintedShares AS DOUBLE) AS ratio
        FROM {{ source('balancer_v3_gnosis', 'Vault_evt_Wrap') }}
        {% if is_incremental() %}
        AND {{ incremental_predicate('evt_block_time') }}
        {% endif %}      

        UNION ALL

        SELECT 
            evt_block_time,
            underlyingToken,
            wrappedToken, 
            CAST(withdrawnUnderlying AS DOUBLE) / CAST(burnedShares AS DOUBLE) AS ratio
        FROM {{ source('balancer_v3_gnosis', 'Vault_evt_Unwrap') }}    
        {% if is_incremental() %}
        AND {{ incremental_predicate('evt_block_time') }}
        {% endif %}      
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
    'gnosis' AS blockchain,
    p.wrappedToken AS wrapped_token,
    p.underlyingToken AS underlying_token,
    m.erc4626TokenSymbol AS erc4626_token_symbol,
    m.underlyingTokenSymbol AS underlying_token_symbol,
    p.decimals AS decimals,
    APPROX_PERCENTILE(adjusted_price, 0.5) AS median_price,
    LEAD(DATE_TRUNC('day', p.evt_block_time), 1, NOW()) OVER (PARTITION BY p.underlyingToken ORDER BY p.evt_block_time) AS next_change
FROM price_join p
JOIN {{ref('balancer_v3_gnosis_erc4626_token_mapping')}} m ON m.underlying_token = p.underlyingToken
GROUP BY 1, 2, 3, 4, 5, 6