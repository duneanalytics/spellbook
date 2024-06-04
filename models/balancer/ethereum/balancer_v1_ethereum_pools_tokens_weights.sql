{{
    config(
        schema='balancer_v1_ethereum',
        alias = 'pools_tokens_weights',
        
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['pool_id', 'token_address'],
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "balancer_v1",
                                    \'["metacrypto", "jacektrocinski"]\') }}'
    )
}}

{% set bind_start_date =  '2020-02-28' %}
{% set rebind_start_date = '2020-04-01' %}
{% set unbind_start_date = '2020-04-05' %}

WITH events AS (
    -- Binds
    SELECT
        bind.call_block_number AS block_number,
        tx.index,
        bind.call_trace_address,
        bind.contract_address AS pool,
        bind.token,
        CAST(bind.denorm as int256) as denorm
    FROM {{ source('balancer_v1_ethereum', 'BPool_call_bind') }} bind
    INNER JOIN {{ source('ethereum', 'transactions') }} tx ON tx.hash = bind.call_tx_hash 
    WHERE bind.call_success = TRUE
        {% if not is_incremental() %}
        AND bind.call_block_time >= TIMESTAMP '{{bind_start_date}}'
        AND tx.block_time >= TIMESTAMP '{{bind_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND bind.call_block_time >= date_trunc('day', now() - interval '7' day)
        AND tx.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}

    UNION ALL

    -- Rebinds
    SELECT
        rebind.call_block_number AS block_number,
        tx.index,
        rebind.call_trace_address,
        rebind.contract_address AS pool,
        rebind.token,
        CAST(rebind.denorm as int256) as denorm
    FROM {{ source('balancer_v1_ethereum', 'BPool_call_rebind') }} rebind
    INNER JOIN {{ source('ethereum', 'transactions') }} tx ON tx.hash = rebind.call_tx_hash 
    WHERE rebind.call_success = TRUE
        {% if not is_incremental() %}
        AND rebind.call_block_time >= TIMESTAMP '{{bind_start_date}}'
        AND tx.block_time >= TIMESTAMP '{{bind_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND rebind.call_block_time >= date_trunc('day', now() - interval '7' day)
        AND tx.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    
    UNION ALL
    
    -- Unbinds
    SELECT
        unbind.call_block_number AS block_number, 
        tx.index,
        unbind.call_trace_address,
        unbind.contract_address AS pool,
        unbind.token,
        CAST('0' as int256) AS denorm
    FROM {{ source('balancer_v1_ethereum', 'BPool_call_unbind') }} unbind
    INNER JOIN {{ source('ethereum', 'transactions') }} tx ON tx.hash = unbind.call_tx_hash 
    WHERE unbind.call_success = TRUE
        {% if not is_incremental() %}
        AND unbind.call_block_time >= TIMESTAMP '{{bind_start_date}}'
        AND tx.block_time >= TIMESTAMP '{{bind_start_date}}'
        {% endif %}
        {% if is_incremental() %}
        AND unbind.call_block_time >= date_trunc('day', now() - interval '7' day)
        AND tx.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
),
state_with_gaps AS (
    SELECT
        events.block_number,
        events.pool,
        events.token,
        events.denorm,
        LEAD(events.block_number, 1) OVER (
            PARTITION BY events.pool, events.token 
            ORDER BY events.block_number, index, call_trace_address
        ) AS next_block_number
    FROM events 
), 
settings AS (
    SELECT
        pool, 
        token, 
        denorm
    FROM state_with_gaps
    WHERE
        next_block_number IS NULL
        AND denorm <> CAST('0' as int256)
),
sum_denorm AS (
    SELECT
        pool,
        SUM(denorm) AS sum_denorm
    FROM state_with_gaps
    WHERE
        next_block_number IS NULL
        AND denorm <> CAST('0' as int256)
    GROUP BY pool
),
norm_weights AS (
    SELECT
        settings.pool AS pool_address,
        token AS token_address,
        CAST(denorm as DOUBLE) / CAST(sum_denorm AS DOUBLE) AS normalized_weight
    FROM settings
    INNER JOIN sum_denorm ON settings.pool = sum_denorm.pool
)
--
-- Balancer v1 Pools Tokens Weights
--
SELECT
    'ethereum' AS blockchain,
    '1' AS version,
    pool_address AS pool_id,
    token_address,
    normalized_weight
FROM norm_weights