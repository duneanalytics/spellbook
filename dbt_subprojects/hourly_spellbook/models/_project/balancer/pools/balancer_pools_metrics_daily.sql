{{ config(
    tags = ['prod_exclude'],
    schema = 'balancer',
    alias = 'pools_metrics_daily',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'project_contract_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , post_hook='{{ hide_spells() }}'
    )
}}


WITH 
trades AS(
    SELECT 
        block_date,
        version,
        blockchain,
        pool_id,
        project_contract_address,
        sum(amount_usd) AS swap_amount_usd
    FROM {{ source('balancer', 'trades') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('block_date')}}
    {% endif %}
    GROUP BY 1, 2, 3, 4, 5
),

liquidity AS(
    SELECT
        day AS block_date,
        blockchain,
        version,
        pool_id,
        pool_address AS project_contract_address,
        pool_type,
        pool_symbol,
        sum(pool_liquidity_usd) AS tvl_usd,
        sum(pool_liquidity_eth) AS tvl_eth
    FROM {{ ref('balancer_liquidity') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('day')}}
    {% endif %}
    GROUP BY 1, 2, 3, 4, 5, 6, 7
),

fees AS(
    SELECT
        day,
        version,
        blockchain,
        pool_address,
        sum(protocol_fee_collected_usd) AS fee_amount_usd
    FROM {{ ref('balancer_protocol_fee') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('day')}}
    {% endif %}
    GROUP BY 1, 2, 3, 4
)

SELECT
    l.blockchain,
    'balancer' AS project,
    l.version,
    l.block_date,
    l.project_contract_address,
    l.pool_symbol,
    l.pool_type,
    t.swap_amount_usd,
    l.tvl_usd,
    l.tvl_eth,
    f.fee_amount_usd
FROM liquidity l
LEFT JOIN trades t ON l.block_date = t.block_date
AND l.blockchain = t.blockchain
AND l.version = t.version
-- v2/v3 use pool_id as the canonical join key across liquidity/trades
AND (
    l.pool_id = t.pool_id
    -- v1 trades do not always carry pool_id; fallback to address matching
    OR (l.pool_id IS NULL AND l.project_contract_address = t.project_contract_address)
)
LEFT JOIN fees f ON l.block_date = f.day
AND l.blockchain = f.blockchain
AND l.version = f.version
-- v2/v3 use pool_id as the canonical join key across liquidity/fees
AND (
    l.pool_id = f.pool_id
    -- v1 fees may only map by pool address
    OR (l.pool_id IS NULL AND l.project_contract_address = f.pool_address)
)
ORDER BY 1 DESC, 7 DESC
