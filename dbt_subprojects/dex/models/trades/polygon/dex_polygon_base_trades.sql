{{ config(
    schema = 'dex_polygon'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set base_models = [
    ref('uniswap_v4_polygon_base_trades')
    , ref('uniswap_v3_polygon_base_trades')
    , ref('uniswap_v2_polygon_base_trades')
    , ref('apeswap_polygon_base_trades')
    , ref('airswap_polygon_base_trades')
    , ref('sushiswap_v1_polygon_base_trades')
    , ref('sushiswap_v2_polygon_base_trades')
    , ref('honeyswap_v2_polygon_base_trades')
    , ref('quickswap_v2_polygon_base_trades')
    , ref('quickswap_v3_polygon_base_trades')
    , ref('balancer_v2_polygon_base_trades')
    , ref('fraxswap_polygon_base_trades')
    , ref('dodo_polygon_base_trades')
    , ref('kyberswap_polygon_base_trades')
    , ref('clipper_polygon_base_trades')
    , ref('xchange_polygon_base_trades')
    , ref('dooar_polygon_base_trades')
    , ref('smardex_polygon_base_trades')
    , ref('gridex_polygon_base_trades')
    , ref('swaap_v2_polygon_base_trades')
    , ref('dfyn_polygon_base_trades')
    , ref('jetswap_polygon_base_trades')
    , ref('gravity_finance_polygon_base_trades')
    , ref('fluid_v1_polygon_base_trades')
] %}
with base_union as (
    SELECT *
    FROM
    (
        {% for base_model in base_models %}
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
            , row_number() over (partition by tx_hash, evt_index order by tx_hash) as duplicates_rank
        FROM
            {{ base_model }}
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

{{
    add_tx_columns(
        model_cte = 'base_union'
        , blockchain = 'polygon'
        , columns = ['from', 'to', 'index']
    )
}}
