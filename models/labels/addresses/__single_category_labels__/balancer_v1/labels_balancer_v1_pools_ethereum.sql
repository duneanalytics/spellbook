{{config(
    
    alias = 'balancer_v1_pools_ethereum',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['address'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                     "sector",
                                    "labels",
                                    \'["balancerlabs"]\') }}'
    )
}}

WITH events AS (
    -- binds
    SELECT call_block_number AS block_number,
           contract_address  AS pool,
           token,
           denorm
    FROM {{ source('balancer_v1_ethereum', 'BPool_call_bind') }}
    WHERE call_success
    {% if is_incremental() %}
        AND call_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    UNION all

    -- rebinds
    SELECT call_block_number AS block_number,
           contract_address  AS pool,
           token,
           denorm
    FROM {{ source('balancer_v1_ethereum', 'BPool_call_rebind') }}
    WHERE call_success
    {% if is_incremental() %}
        AND call_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}

    UNION all

    -- unbinds
    SELECT call_block_number AS block_number,
            contract_address AS pool,
            token,
            uint256 '0' AS denorm
    FROM {{ source('balancer_v1_ethereum', 'BPool_call_unbind') }}
    WHERE call_success
    {% if is_incremental() %}
        AND call_block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}
),

state_with_gaps AS (
    SELECT events.block_number
           , events.pool
           , events.token
           , CAST(events.denorm AS uint256) AS denorm,
    LEAD(events.block_number, 1, 99999999) OVER (PARTITION BY events.pool, events.token ORDER BY events.block_number) AS next_block_number
    FROM events
),

settings AS (
    SELECT pool,
    coalesce(t.symbol,'?') AS symbol,
    denorm,
    next_block_number
    FROM state_with_gaps s
    LEFT JOIN {{ source('tokens_ethereum', 'erc20') }} t ON s.token = t.contract_address
    WHERE next_block_number = 99999999
    AND denorm > uint256 '0'
),

final AS (
    SELECT
      'ethereum' AS blockchain,
      pool AS address,
      lower(concat(array_join(array_agg(symbol), '/'), ' ', array_join(array_agg(cast(norm_weight AS varchar)), '/'))) AS name,
      'balancer_v1_pool' AS category,
      'balancerlabs' AS contributor,
      'query' AS source,
      timestamp '2023-02-02' AS created_at,
      now() AS updated_at,
      'balancer_v1_pools_ethereum' AS model_name,
      'identifier' as label_type
    FROM   (
        SELECT s1.pool, symbol, cast(100*denorm/total_denorm AS integer) AS norm_weight FROM settings s1
        INNER JOIN (SELECT pool, sum(denorm) AS total_denorm FROM settings GROUP BY pool) s2
        ON s1.pool = s2.pool
        ORDER BY 1 ASC , 3 DESC, 2 ASC
    ) s

    GROUP BY 1, 2
)
SELECT *
FROM final
WHERE length(name) < 35