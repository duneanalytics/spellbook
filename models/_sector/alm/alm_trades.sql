{{ config(
    schema = 'alm'
    , alias = 'trades'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'vault_address']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set models = [
    ref('alm_ethereum_trades')
] %}

WITH base_union AS (
    SELECT *
    FROM (
        {% for model in models %}
        SELECT
            blockchain
            , project
            , version
            , dex
            , dex_version
            , block_time
            , block_month
            , block_number
            , pool_address
            , vault_address
            , token_pair
            , token0_address
            , token1_address
            , volume_usd
            , volume_share
            , swap_volume_usd
            , volume0
            , swap_volume0
            , token0_symbol
            , volume1
            , swap_volume1
            , token1_symbol
            , volume0_raw
            , volume1_raw
            , tx_hash
            , evt_index
            , row_number() over (partition by tx_hash, evt_index, vault_address order by tx_hash asc, evt_index asc) as duplicates_rank
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
