{{ config(
    schema = 'balancer',
    alias = 'pools_stats',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'project_contract_address'],
    incremental_predicates = ['DBT_INTERNAL_DEST.block_date >= date_trunc(\'day\', now() - interval \'7\' day)'],
    post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "ethereum", "gnosis", "optimism", "polygon", "zkevm"]\',
                            "project",
                            "balancer",
                            \'["viniabussafi"]\') }}'
    )
}}


WITH 
trades AS(
    SELECT 
        block_date,
        version,
        blockchain,
        project_contract_address,
        pool_type,
        pool_symbol,
        sum(amount_usd) AS swap_amount_usd_usd
    FROM {{ ref('balancer_trades') }}
    {% if is_incremental() %}
    WHERE block_date >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    GROUP BY 1, 2, 3, 4, 5, 6
),

liquidity AS(
    SELECT
        day,
        blockchain,
        pool_address,
        sum(pool_liquidity_usd) AS tvl_usd,
        sum(pool_liquidity_eth) AS tvl_eth
    FROM {{ ref('balancer_liquidity') }}
    {% if is_incremental() %}
    WHERE day >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    GROUP BY 1, 2, 3
),

fees AS(
    SELECT
        day,
        blockchain,
        pool_address,
        sum(protocol_fee_collected_usd) AS fee_amount_usd
    FROM {{ ref('balancer_protocol_fee') }}
    {% if is_incremental() %}
    WHERE day >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    GROUP BY 1, 2, 3 
)

SELECT
    t.blockchain,
    'balancer' AS project,
    t.version,
    t.block_date,
    t.project_contract_address,
    t.pool_symbol,
    t.pool_type,
    t.swap_amount_usd,
    l.tvl_usd,
    l.tvl_eth,
    f.fee_amount_usd
FROM trades t
LEFT JOIN liquidity l ON block_date = l.day
AND t.project_contract_address = l.pool_address 
AND t.blockchain = l.blockchain
LEFT JOIN fees f ON block_date = f.day
AND t.project_contract_address = f.pool_address 
AND t.blockchain = f.blockchain
ORDER BY 1 DESC, 7 DESC
;