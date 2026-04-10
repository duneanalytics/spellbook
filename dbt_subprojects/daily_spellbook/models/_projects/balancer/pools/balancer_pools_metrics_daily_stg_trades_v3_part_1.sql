{{ config(
    schema = 'balancer',
    alias = 'pools_metrics_daily_stg_trades_v3_part_1',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'version', 'trade_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    post_hook='{{ hide_spells() }}'
    )
}}

{% set v3_trade_sources = [
    'balancer_v3_ethereum',
    'balancer_v3_arbitrum',
    'balancer_v3_base',
    'balancer_v3_avalanche_c'
] %}

WITH raw_trades AS(
    {% for source_name in v3_trade_sources %}
    SELECT
        block_date,
        version,
        blockchain,
        pool_id,
        project_contract_address,
        amount_usd
    FROM {{ source(source_name, 'trades') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_date') }}
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)

SELECT
    block_date,
    version,
    blockchain,
    coalesce(pool_id, project_contract_address) AS trade_key,
    pool_id,
    project_contract_address,
    sum(amount_usd) AS swap_amount_usd
FROM raw_trades
GROUP BY 1, 2, 3, 4, 5, 6
