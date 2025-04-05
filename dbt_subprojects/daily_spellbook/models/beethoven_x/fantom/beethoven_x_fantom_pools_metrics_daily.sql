{{ config(
    schema = 'beethoven_x_fantom',
    alias = 'pools_metrics_daily',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'project', 'version', 'project_contract_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    post_hook='{{ expose_spells(blockchains = \'["arbitrum", "avalanche_c", "base", "ethereum", "gnosis", "optimism", "polygon", "zkevm"]\',
                            spell_type = "project",
                            spell_name = "beethoven_x_fantom",
                            contributors = \'["viniabussafi", "metacrypto"]\') }}'
    )
}}


WITH 
trades AS(
    SELECT 
        block_date,
        version,
        blockchain,
        project_contract_address,
        sum(amount_usd) AS swap_amount_usd
    FROM {{ source('beethoven_x', 'trades') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('block_date')}}
    {% endif %}
    GROUP BY 1, 2, 3, 4
),

liquidity AS(
    SELECT
        day AS block_date,
        blockchain,
        version,
        pool_address AS project_contract_address,
        pool_type,
        pool_symbol,
        sum(pool_liquidity_usd) AS tvl_usd,
        sum(pool_liquidity_eth) AS tvl_eth
    FROM {{ ref('beethoven_x_fantom_liquidity') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('day')}}
    {% endif %}
    GROUP BY 1, 2, 3, 4, 5, 6
),

fees AS(
    SELECT
        day,
        version,
        blockchain,
        pool_address,
        sum(protocol_fee_collected_usd) AS fee_amount_usd
    FROM {{ ref('beethoven_x_fantom_protocol_fee') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('day')}}
    {% endif %}
    GROUP BY 1, 2, 3, 4
)

SELECT
    l.blockchain,
    'beethoven_x_fantom' AS project,
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
AND l.project_contract_address = t.project_contract_address 
AND l.blockchain = t.blockchain
LEFT JOIN fees f ON l.block_date = f.day
AND l.project_contract_address = f.pool_address 
AND l.blockchain = f.blockchain
ORDER BY 1 DESC, 7 DESC
