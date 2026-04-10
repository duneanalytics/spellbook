{{ config(
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
        swap_amount_usd
    FROM {{ ref('balancer_pools_metrics_daily_stg_trades_v1') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('block_date')}}
    {% endif %}

    UNION ALL

    SELECT
        block_date,
        version,
        blockchain,
        pool_id,
        project_contract_address,
        swap_amount_usd
    FROM {{ ref('balancer_pools_metrics_daily_stg_trades_v2') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('block_date')}}
    {% endif %}

    UNION ALL

    SELECT
        block_date,
        version,
        blockchain,
        pool_id,
        project_contract_address,
        swap_amount_usd
    FROM {{ ref('balancer_pools_metrics_daily_stg_trades_v3_part_1') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('block_date')}}
    {% endif %}

    UNION ALL

    SELECT
        block_date,
        version,
        blockchain,
        pool_id,
        project_contract_address,
        swap_amount_usd
    FROM {{ ref('balancer_pools_metrics_daily_stg_trades_v3_part_2') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('block_date')}}
    {% endif %}

    UNION ALL

    SELECT
        block_date,
        version,
        blockchain,
        pool_id,
        project_contract_address,
        swap_amount_usd
    FROM {{ ref('balancer_pools_metrics_daily_stg_trades_v3_part_3') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('block_date')}}
    {% endif %}
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
        pool_id,
        pool_address,
        sum(protocol_fee_collected_usd) AS fee_amount_usd
    FROM {{ ref('balancer_protocol_fee') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('day')}}
    {% endif %}
    GROUP BY 1, 2, 3, 4, 5
)

SELECT
    l.blockchain,
    'balancer' AS project,
    l.version,
    l.block_date,
    l.project_contract_address,
    COALESCE(l.pool_symbol, erc.symbol) AS pool_symbol,
    l.pool_type,
    COALESCE(t.swap_amount_usd, 0) AS swap_amount_usd,
    l.tvl_usd,
    l.tvl_eth,
    COALESCE(f.fee_amount_usd, 0) AS fee_amount_usd
FROM liquidity l
LEFT JOIN trades t ON l.block_date = t.block_date
AND l.blockchain = t.blockchain
AND l.version = t.version
AND CASE
        WHEN l.pool_id IS NULL THEN l.project_contract_address
        ELSE l.pool_id
    END = CASE
        WHEN l.pool_id IS NULL THEN t.project_contract_address
        ELSE t.pool_id
    END
LEFT JOIN fees f ON l.block_date = f.day
AND l.blockchain = f.blockchain
AND l.version = f.version
AND CASE
        WHEN l.pool_id IS NULL THEN l.project_contract_address
        ELSE l.pool_id
    END = CASE
        WHEN l.pool_id IS NULL THEN f.pool_address
        ELSE f.pool_id
    END
LEFT JOIN {{ source('tokens', 'erc20') }} erc
    ON l.project_contract_address = erc.contract_address
    AND l.blockchain = erc.blockchain
ORDER BY 1 DESC, 7 DESC
