{{ config(
    schema = 'dex'
    , alias = 'base_trades'
    , materialized = 'view'
    )
}}

{% set models = [  
      ref('dex_arbitrum_base_trades')
    , ref('dex_avalanche_c_base_trades')
    , ref('dex_abstract_base_trades')
    , ref('dex_base_base_trades')
    , ref('dex_berachain_base_trades')
    , ref('dex_blast_base_trades')
    , ref('dex_bnb_base_trades')
    , ref('dex_boba_base_trades')
    , ref('dex_celo_base_trades')
    , ref('dex_corn_base_trades')
    , ref('dex_ethereum_base_trades')
    , ref('dex_fantom_base_trades')
    , ref('dex_flare_base_trades')
    , ref('dex_gnosis_base_trades')
    , ref('dex_hemi_base_trades')
    , ref('dex_ink_base_trades')
    , ref('dex_linea_base_trades')
    , ref('dex_kaia_base_trades')
    , ref('dex_katana_base_trades')
    , ref('dex_mantle_base_trades')
    , ref('dex_nova_base_trades')
    , ref('dex_opbnb_base_trades')
    , ref('dex_optimism_base_trades')
    , ref('dex_plume_base_trades')
    , ref('dex_polygon_base_trades')
    , ref('dex_ronin_base_trades')
    , ref('dex_scroll_base_trades')
    , ref('dex_sei_base_trades')
    , ref('dex_shape_base_trades')
    , ref('dex_sonic_base_trades')
    , ref('dex_sophon_base_trades')
    , ref('dex_superseed_base_trades')
    , ref('dex_taiko_base_trades')
    , ref('dex_unichain_base_trades')
    , ref('dex_worldchain_base_trades')
    , ref('dex_zkevm_base_trades')
    , ref('dex_zksync_base_trades')
    , ref('dex_zora_base_trades')
] %}

with base_union as (
    {% for model in models %}
    SELECT
        blockchain
        , project
        , version
        , block_month
        , block_date
        , block_time
        , block_number
        , token_bought_amount_raw
        , token_sold_amount_raw
        , token_bought_address
        , token_sold_address
        , taker
        , maker
        , project_contract_address
        , tx_hash
        , evt_index
        , tx_from
        , tx_to
        , tx_index
    FROM
        {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
select
    *
from
    base_union
