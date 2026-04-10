{{ config(
    schema = 'balancer',
    alias = 'pools_metrics_daily_stg_trades_v3_part_2',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'version', 'trade_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    views_enabled = false,
    post_hook='{{ hide_spells() }}'
    )
}}

SELECT
    block_date,
    version,
    blockchain,
    coalesce(pool_id, project_contract_address) AS trade_key,
    pool_id,
    project_contract_address,
    sum(amount_usd) AS swap_amount_usd
FROM {{ source('balancer', 'trades') }}
WHERE version = '3'
AND blockchain NOT IN ('ethereum', 'arbitrum', 'base', 'avalanche_c')
{% if is_incremental() %}
AND {{ incremental_predicate('block_date') }}
{% endif %}
GROUP BY 1, 2, 3, 4, 5, 6
