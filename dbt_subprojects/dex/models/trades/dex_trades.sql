{{ config(
    schema = 'dex'
    , alias = 'trades'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    , post_hook='{{ expose_spells(\'[
                                        "abstract"
                                        , "apechain"
                                        , "arbitrum"
                                        , "avalanche_c"
                                        , "base"
                                        , "berachain"
                                        , "blast"
                                        , "bnb"
                                        , "boba"
                                        , "celo"
                                        , "corn"
                                        , "ethereum"
                                        , "fantom"
                                        , "flare"
                                        , "flow"
                                        , "gnosis"
                                        , "hemi"
                                        , "hyperevm"
                                        , "ink"
                                        , "kaia"
                                        , "katana"
                                        , "linea"
                                        , "mantle"
                                        , "nova"
                                        , "opbnb"
                                        , "optimism"
                                        , "peaq"
                                        , "plasma"
                                        , "plume"
                                        , "polygon"
                                        , "ronin"
                                        , "scroll"
                                        , "sei"
                                        , "shape"
                                        , "somnia"
                                        , "sonic"
                                        , "sophon"
                                        , "superseed"
                                        , "taiko"
                                        , "unichain"
                                        , "worldchain"
                                        , "zkevm"
                                        , "zksync"
                                        , "zora"
                                    ]\',
                                    "sector",
                                    "dex",
                                    \'["hosuke", "0xrob", "jeff-dude", "tomfutago", "viniabussafi", "krishhh"]\') }}')
}}

-- keep existing dbt lineages for the following projects, as the team built themselves and use the spells throughout the entire lineage.
{% set as_is_models = [
    ref('oneinch_lop_own_trades')
    , ref('zeroex_native_trades')
] %}

{% set chain_models = [
      ref('dex_abstract_trades')
    , ref('dex_apechain_trades')
    , ref('dex_arbitrum_trades')
    , ref('dex_avalanche_c_trades')
    , ref('dex_base_trades')
    , ref('dex_berachain_trades')
    , ref('dex_blast_trades')
    , ref('dex_bnb_trades')
    , ref('dex_boba_trades')
    , ref('dex_celo_trades')
    , ref('dex_corn_trades')
    , ref('dex_ethereum_trades')
    , ref('dex_fantom_trades')
    , ref('dex_flare_trades')
    , ref('dex_flow_trades')
    , ref('dex_gnosis_trades')
    , ref('dex_hemi_trades')
    , ref('dex_hyperevm_trades')
    , ref('dex_ink_trades')
    , ref('dex_kaia_trades')
    , ref('dex_katana_trades')
    , ref('dex_linea_trades')
    , ref('dex_mantle_trades')
    , ref('dex_mezo_trades')
    , ref('dex_monad_trades')
    , ref('dex_nova_trades')
    , ref('dex_opbnb_trades')
    , ref('dex_optimism_trades')
    , ref('dex_peaq_trades')
    , ref('dex_plasma_trades')
    , ref('dex_plume_trades')
    , ref('dex_polygon_trades')
    , ref('dex_ronin_trades')
    , ref('dex_scroll_trades')
    , ref('dex_sei_trades')
    , ref('dex_shape_trades')
    , ref('dex_somnia_trades')
    , ref('dex_sonic_trades')
    , ref('dex_sophon_trades')
    , ref('dex_story_trades')
    , ref('dex_superseed_trades')
    , ref('dex_tac_trades')
    , ref('dex_taiko_trades')
    , ref('dex_unichain_trades')
    , ref('dex_worldchain_trades')
    , ref('dex_zkevm_trades')
    , ref('dex_zksync_trades')
    , ref('dex_zora_trades')
] %}

WITH chain_trades AS (
    {% for model in chain_models %}
    SELECT
        blockchain
        , project
        , version
        , block_month
        , block_date
        , block_time
        , block_number
        , token_bought_symbol
        , token_sold_symbol
        , token_pair
        , token_bought_amount
        , token_sold_amount
        , token_bought_amount_raw
        , token_sold_amount_raw
        , amount_usd
        , token_bought_address
        , token_sold_address
        , taker
        , maker
        , project_contract_address
        , tx_hash
        , tx_from
        , tx_to
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
, as_is_dexs AS (
    {% for model in as_is_models %}
    SELECT
        blockchain
        , project
        , version
        , block_month
        , block_date
        , block_time
        , block_number
        , token_bought_symbol
        , token_sold_symbol
        , token_pair
        , token_bought_amount
        , token_sold_amount
        , token_bought_amount_raw
        , token_sold_amount_raw
        , amount_usd
        , token_bought_address
        , token_sold_address
        , taker
        , maker
        , project_contract_address
        , tx_hash
        , tx_from
        , tx_to
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

{% set cte_to_union = [
    'chain_trades'
    , 'as_is_dexs'
    ]
%}

{% for cte in cte_to_union %}
    SELECT
        blockchain
        , project
        , version
        , block_month
        , block_date
        , block_time
        , block_number
        , token_bought_symbol
        , token_sold_symbol
        , token_pair
        , token_bought_amount
        , token_sold_amount
        , token_bought_amount_raw
        , token_sold_amount_raw
        , amount_usd
        , token_bought_address
        , token_sold_address
        , taker
        , maker
        , project_contract_address
        , tx_hash
        , tx_from
        , tx_to
        , evt_index
    FROM
        {{ cte }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
