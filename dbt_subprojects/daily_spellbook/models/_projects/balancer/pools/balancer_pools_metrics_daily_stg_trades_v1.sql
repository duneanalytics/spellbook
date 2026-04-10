{{ config(
    schema = 'balancer',
    alias = 'pools_metrics_daily_stg_trades_v1',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'blockchain', 'version', 'trade_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    post_hook='{{ hide_spells() }}'
    )
}}

WITH raw_trades AS(
    SELECT
        block_date,
        version,
        blockchain,
        cast(NULL AS varbinary) AS pool_id,
        project_contract_address,
        amount_usd
    FROM {{ source('dex', 'trades') }}
    WHERE project = 'balancer'
    AND version = '1'
    {% if is_incremental() %}
    AND {{ incremental_predicate('block_date') }}
    {% endif %}

    UNION ALL

    SELECT
        block_date,
        version,
        blockchain,
        pool_id,
        project_contract_address,
        amount_usd
    FROM {{ source('balancer_cowswap_amm', 'trades') }}
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_date') }}
    {% endif %}
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
