{{ config(
        schema = 'balancer_v3_base',
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
            wrappedToken,
            CAST(depositedUnderlying AS DOUBLE) / CAST(mintedShares AS DOUBLE) AS ratio
        FROM {{ source('balancer_v3_base', 'Vault_evt_Wrap') }}
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('evt_block_time') }}
        {% endif %}      

        UNION ALL

        SELECT 
            evt_block_time,
            wrappedToken, 
            CAST(withdrawnUnderlying AS DOUBLE) / CAST(burnedShares AS DOUBLE) AS ratio
        FROM {{ source('balancer_v3_base', 'Vault_evt_Unwrap') }}    
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('evt_block_time') }}
        {% endif %}      
    ),


    price_join AS(
    SELECT 
        w.evt_block_time,
        m.underlying_token,
        w.wrappedToken,
        m.erc4626_token_symbol,
        m.underlying_token_symbol,
        m.decimals,
        ratio * price AS adjusted_price
    FROM wrap_unwrap w
    JOIN {{ref('balancer_v3_base_erc4626_token_mapping')}} m ON m.erc4626_token = w.wrappedToken
    JOIN {{ source('prices', 'usd') }} p ON m.underlying_token = p.contract_address
    AND p.blockchain = 'base'
    AND DATE_TRUNC('minute', w.evt_block_time) = DATE_TRUNC('minute', p.minute)
    {% if is_incremental() %}
    AND {{ incremental_predicate('p.minute') }}
    {% endif %}          
    )

SELECT
    p.evt_block_time AS minute,
    'base' AS blockchain,
    wrappedToken AS wrapped_token,
    underlying_token,
    erc4626_token_symbol,
    underlying_token_symbol,
    decimals,
    APPROX_PERCENTILE(adjusted_price, 0.5) AS median_price,
    LEAD(p.evt_block_time, 1, CURRENT_DATE + INTERVAL '1' day) OVER (PARTITION BY wrappedToken ORDER BY p.evt_block_time) AS next_change
FROM price_join p
GROUP BY 1, 2, 3, 4, 5, 6, 7
