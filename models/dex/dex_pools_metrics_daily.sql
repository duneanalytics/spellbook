{{ config(
        
        alias = 'pools_metrics_daily',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'blockchain', 'project', 'version', 'project_contract_address'],
        incremental_predicates = ['DBT_INTERNAL_DEST.block_date >= date_trunc(\'day\', now() - interval \'7\' day)'],
        post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "ethereum", "gnosis", "optimism", "polygon", "zkevm"]\',
                                "sector",
                                "dex",
                                \'["viniabussafi"]\') }}'
        )
}}

{% set dex_pool_metrics_models = [
 ref('balancer_pools_metrics')
] %}


SELECT  *
FROM (
    {% for dex_model in dex_pool_metrics_models %}
    SELECT
        blockchain,
        project,
        version,
        block_date,
        project_contract_address,
        pool_symbol,
        pool_type,
        swap_amount_usd,
        tvl_usd,
        tvl_eth,
        fee_amount_usd
    FROM {{ dex_model }}
    {% if is_incremental() %}
    WHERE block_date >= date_trunc('day', now() - interval '7' day)
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
