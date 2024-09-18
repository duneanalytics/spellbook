{{ config(
        schema = 'dex',
        alias = 'pools_metrics_daily',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'blockchain', 'project', 'version', 'project_contract_address'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
        post_hook='{{ expose_spells(blockchains = \'["arbitrum", "avalanche_c", "base", "ethereum", "gnosis", "optimism", "polygon", "zkevm"]\',
                                spell_type = "sector",
                                spell_name = "dex",
                                contributors = \'["viniabussafi", "metacrypto"]\') }}'
        )
}}

{% set dex_pool_metrics_models = [
 source('balancer','pools_metrics_daily'),
 source('beethoven_x_fantom','pools_metrics_daily'),
 source('jelly_swap_sei','pools_metrics_daily')
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
    WHERE {{incremental_predicate('block_date')}}
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
