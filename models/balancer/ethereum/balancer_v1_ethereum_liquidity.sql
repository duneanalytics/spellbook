{{
    config(
        schema='balancer_v1_ethereum',
        alias = 'liquidity',
        
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['day', 'pool_id', 'token_address'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "balancer_v1",
                                    \'["stefenon", "viniabussafi"]\') }}'
    )
}}

WITH prices AS (
        SELECT
            date_trunc('day', minute) AS day,
            contract_address AS token,
            AVG(price) AS price
        FROM {{ source('prices', 'usd') }}
        WHERE blockchain = 'ethereum'
        {% if is_incremental() %}
        AND minute >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        GROUP BY 1, 2
    ),

    dex_prices_1 AS (
       SELECT
            date_trunc('day', hour) AS day,
            contract_address AS token,
            approx_percentile(median_price, 0.5) AS price,
            SUM(sample_size) AS sample_size 
        FROM {{ ref('dex_prices') }}
        WHERE blockchain = 'ethereum'
        {% if is_incremental() %}
        AND hour >= date_trunc('day', now() - interval '7' day)
        {% endif %}
        GROUP BY 1, 2
        HAVING SUM(sample_size) > 5
        AND AVG(median_price) < 1e8
    ),
    
    dex_prices AS (
       SELECT
            *,
            LEAD(day, 1, NOW()) OVER (
                PARTITION BY token
                ORDER BY
                    day
            ) AS day_of_next_change
        FROM dex_prices_1
    ),

    
    cumulative_balance AS (
        SELECT
            day,
            pool,
            token,
            cumulative_amount
        FROM {{ ref('balancer_ethereum_balances') }} b
        {% if is_incremental() %}
        WHERE day >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    ),
    
   cumulative_usd_balance AS (
        SELECT
            b.day,
            b.pool,
            b.token,
            t.symbol,
            cumulative_amount / POWER(10, t.decimals) * COALESCE(p1.price, /*p2.price,*/ 0) AS amount_usd
        FROM cumulative_balance b
        LEFT JOIN {{ ref('tokens_ethereum_erc20') }} t ON t.contract_address = b.token
        LEFT JOIN prices p1 ON p1.day = b.day
        AND p1.token = b.token
        LEFT JOIN dex_prices p2 ON p2.day <= b.day
        AND b.day < p2.day_of_next_change
        AND p2.token = b.token
    ),
    
    pool_liquidity_estimates AS (
        SELECT
            b.day,
            b.pool,
            SUM(b.amount_usd) / SUM(w.normalized_weight) AS liquidity
        FROM cumulative_usd_balance b
        INNER JOIN {{ ref('balancer_v1_ethereum_pools_tokens_weights') }} w ON b.pool = w.pool_id
        AND b.token = w.token_address
        AND CAST (b.amount_usd as DOUBLE) > CAST (0 as DOUBLE)
        AND CAST (w.normalized_weight as DOUBLE) > CAST (0 as DOUBLE)
        GROUP BY 1, 2
    ),
    
    balancer_liquidity AS (
        SELECT
            b.day,
            w.pool_id,
            {#
            --p.name AS pool_symbol,
            #}
            w.token_address,
            t.symbol AS token_symbol,
            liquidity * normalized_weight AS usd_amount
        FROM pool_liquidity_estimates b
        LEFT JOIN {{ ref('balancer_v1_ethereum_pools_tokens_weights') }} w ON b.pool = w.pool_id
        AND CAST (w.normalized_weight as DOUBLE) > CAST (0 as DOUBLE)
        LEFT JOIN {{ ref('tokens_ethereum_erc20') }} t ON t.contract_address = w.token_address
        {#
        --LEFT JOIN pool_labels p ON p.address = w.pool_id
        #}
    )
    
SELECT * FROM balancer_liquidity ORDER BY 1, 2, 3
