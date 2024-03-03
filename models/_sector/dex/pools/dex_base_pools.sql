{{ config(
    schema = 'dex'
    , alias = 'base_pools'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set models = [
    ref('dex_ethereum_base_pools')
] %}

with base_union as (
    SELECT *
    FROM
    (
        {% for model in models %}
        SELECT
            blockchain
            , project
            , version
            , block_month
            , block_date
            , block_time
            , block_number
            , pool
            , fee
            , token0
            , token1
            , contract_address
            , tx_hash
            , evt_index
        FROM
            {{ model }}
        {% if is_incremental() %}
        WHERE
            {{ incremental_predicate('block_time') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
    )
)
select
    *
from
    base_union
