{{config(
    schema = 'tokens',
    alias = 'transfers',
    partition_by = ['blockchain', 'token_standard', 'block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['blockchain', 'unique_key'],
    post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "base", "bnb", "celo", "ethereum", "fantom", "gnosis", "optimism", "polygon", "zksync", "zora"]\',
                                "sector",
                                "tokens",
                                \'["aalan3", "jeff-dude"]\') }}'
)
}}

{% set models = [
    ref('tokens_arbitrum_transfers')
    , ref('tokens_avalanche_c_transfers')
    , ref('tokens_base_transfers')
    , ref('tokens_bnb_transfers')
    , ref('tokens_celo_transfers')
    , ref('tokens_ethereum_transfers')
    , ref('tokens_fantom_transfers')
    , ref('tokens_gnosis_transfers')
    , ref('tokens_optimism_transfers')
    , ref('tokens_polygon_transfers')
    , ref('tokens_zksync_transfers')
    , ref('tokens_zora_transfers')
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
            , tx_index
            , evt_index
            , trace_address
            , token_standard
            , tx_from
            , tx_to
            , "from"
            , "to"
            , contract_address
            , symbol
            , amount_raw
            , amount
            , usd_price
            , usd_amount
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
