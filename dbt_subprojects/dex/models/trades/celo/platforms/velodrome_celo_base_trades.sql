{{
    config(
        schema = 'velodrome_celo',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set clpool_evt_swap %}
(
    select
        *
    from {{ source('velodrome_multichain', 'clpool_evt_swap') }}
    where lower(chain) like 'celo%'
)
{% endset %}

{% set clfactory_evt_poolcreated %}
(
    select
        *
    from {{ source('velodrome_multichain', 'clfactory_evt_poolcreated') }}
    where lower(chain) like 'celo%'
)
{% endset %}

{{
    uniswap_compatible_v3_trades(
        blockchain = 'celo',
        project = 'velodrome',
        version = '2_cl',
        Pair_evt_Swap = clpool_evt_swap,
        Factory_evt_PoolCreated = clfactory_evt_poolcreated,
        optional_columns = []
    )
}}
