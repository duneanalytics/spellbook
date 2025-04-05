{{ config(
    schema = 'dex'
    , alias = 'base_trades'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set models = [
    ref('dex_arbitrum_base_trades')
    , ref('dex_avalanche_c_base_trades')
    , ref('dex_base_base_trades')
    , ref('dex_blast_base_trades')
    , ref('dex_bnb_base_trades')
    , ref('dex_boba_base_trades')
    , ref('dex_celo_base_trades')
    , ref('dex_corn_base_trades')
    , ref('dex_ethereum_base_trades')
    , ref('dex_fantom_base_trades')
    , ref('dex_flare_base_trades')
    , ref('dex_gnosis_base_trades')
    , ref('dex_ink_base_trades')
    , ref('dex_linea_base_trades')
    , ref('dex_kaia_base_trades')
    , ref('dex_mantle_base_trades')
    , ref('dex_nova_base_trades')
    , ref('dex_optimism_base_trades')
    , ref('dex_polygon_base_trades')
    , ref('dex_ronin_base_trades')
    , ref('dex_scroll_base_trades')
    , ref('dex_sei_base_trades')
    , ref('dex_shape_base_trades')
    , ref('dex_sonic_base_trades')
    , ref('dex_worldchain_base_trades')
    , ref('dex_zkevm_base_trades')
    , ref('dex_zksync_base_trades')
    , ref('dex_unichain_base_trades')
    , ref('dex_zora_base_trades')
    , ref('dex_berachain_base_trades')
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
            , cast(token_bought_amount_raw as uint256) as token_bought_amount_raw
            , cast(token_sold_amount_raw as uint256) as token_sold_amount_raw
            , token_bought_address
            , token_sold_address
            , taker
            , maker
            , project_contract_address
            , tx_hash
            , evt_index
            , tx_from
            , tx_to
            , row_number() over (partition by tx_hash, evt_index order by tx_hash) as duplicates_rank
        FROM
            {{ model }}
        WHERE
           token_sold_amount_raw >= 0 and token_bought_amount_raw >= 0
        {% if is_incremental() %}
            AND {{ incremental_predicate('block_time') }}
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
