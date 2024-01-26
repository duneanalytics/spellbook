{{config(
    tags = ['prod_exclude'],
    schema = 'tokens',
    alias = 'base_transfers',
    partition_by = ['blockchain', 'block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['blockchain', 'unique_key']
)
}}

/*
    --short term, to get prod running, omit bnb due to cluster crashing, add later
    , ref('tokens_bnb_base_transfers')
*/

{% set models = [
    ref('tokens_arbitrum_base_transfers')
    , ref('tokens_avalanche_c_base_transfers')
    , ref('tokens_base_base_transfers')
    , ref('tokens_celo_base_transfers')
    , ref('tokens_ethereum_base_transfers')
    , ref('tokens_fantom_base_transfers')
    , ref('tokens_gnosis_base_transfers')
    , ref('tokens_optimism_base_transfers')
    , ref('tokens_zksync_base_transfers')
    , ref('tokens_zora_base_transfers')
    , ref('tokens_polygon_base_transfers')
] %}

with base_union as (
    SELECT *
    FROM
    (
        {% for model in models %}
        SELECT
            unique_key
            , blockchain
            , block_date
            , block_time
            , block_number
            , tx_hash
            , evt_index
            , trace_address
            , token_standard
            , tx_from
            , tx_to
            , tx_index
            , "from"
            , "to"
            , contract_address
            , amount_raw
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
