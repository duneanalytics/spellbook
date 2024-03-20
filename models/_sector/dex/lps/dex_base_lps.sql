{{ config(
    schema = 'dex'
    , alias = 'base_lps'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set models = [
    ref('dex_ethereum_base_ls')
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
            , block_time
            , block_month
            , block_number
            , token0_amount_raw
            , token1_amount_raw
            , liquidity_amount_raw
            , token0_address
            , token1_address
            , pool_address
            , liquidity_provider
            , position_id
            , tick_lower
            , tick_upper
            , tx_hash
            , evt_index
            , row_number() over (partition by tx_hash, evt_index order by tx_hash) as duplicates_rank
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
    WHERE
        duplicates_rank = 1
)

select
    *
from
    base_union
