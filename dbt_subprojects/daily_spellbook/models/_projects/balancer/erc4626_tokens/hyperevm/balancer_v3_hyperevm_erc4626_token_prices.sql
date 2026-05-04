{{ config(
        schema = 'balancer_v3_hyperevm',
        alias = 'erc4626_token_prices',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['minute', 'wrapped_token'],
        incremental_predicates = ["DBT_INTERNAL_DEST.minute >= date_trunc('day', now() - interval '4' day)"]
    )
}}
-- 1-day lookback ensures the prior terminal row per wrapped_token is reprocessed so next_change is updated when new data arrives

        
WITH wrap_unwrap AS(
        SELECT 
            evt_block_time,
            wrappedToken,
            CAST(depositedUnderlying AS DOUBLE) / NULLIF(CAST(mintedShares AS DOUBLE), 0) AS ratio
        FROM {{ source('balancer_v3_hyperevm', 'Vault_evt_Wrap') }}
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '4' day)
        {% endif %}      

        UNION ALL

        SELECT 
            evt_block_time,
            wrappedToken, 
            CAST(withdrawnUnderlying AS DOUBLE) / NULLIF(CAST(burnedShares AS DOUBLE), 0) AS ratio
        FROM {{ source('balancer_v3_hyperevm', 'Vault_evt_Unwrap') }}    
        {% if is_incremental() %}
        WHERE evt_block_time >= date_trunc('day', now() - interval '4' day)
        {% endif %}      
    ),


    price_join AS(
    SELECT 
        DATE_TRUNC('minute', w.evt_block_time) AS minute,
        m.underlying_token,
        w.wrappedToken,
        m.erc4626_token_symbol,
        m.underlying_token_symbol,
        m.decimals,
        ratio * price * POWER(10, (m.decimals - p.decimals)) AS adjusted_price
    FROM wrap_unwrap w
    JOIN {{ref('balancer_v3_hyperevm_erc4626_token_mapping')}} m ON m.erc4626_token = w.wrappedToken
    JOIN {{ source('prices', 'usd') }} p ON m.underlying_token = p.contract_address
    AND p.blockchain = 'hyperevm'
    AND DATE_TRUNC('minute', w.evt_block_time) = DATE_TRUNC('minute', p.minute)
    {% if is_incremental() %}
    AND p.minute >= date_trunc('day', now() - interval '4' day)
    {% endif %}            
    )

SELECT
    p.minute,
    'hyperevm' AS blockchain,
    wrappedToken AS wrapped_token,
    underlying_token,
    erc4626_token_symbol,
    underlying_token_symbol,
    decimals,
    APPROX_PERCENTILE(adjusted_price, 0.5) AS median_price,
    LEAD(p.minute, 1, TIMESTAMP '9999-12-31 23:59:59') OVER (PARTITION BY wrappedToken ORDER BY p.minute) AS next_change
FROM price_join p
GROUP BY 1, 2, 3, 4, 5, 6, 7
