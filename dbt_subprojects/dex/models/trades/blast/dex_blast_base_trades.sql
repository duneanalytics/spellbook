{{ config(
    schema = 'dex_blast'
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
    ref('uniswap_v4_blast_base_trades')
    , ref('uniswap_v3_blast_base_trades')
    , ref('uniswap_v2_blast_base_trades')
    , ref('blasterswap_blast_base_trades')
    , ref('thruster_blast_base_trades')
    , ref('fenix_blast_base_trades')
    , ref('dackieswap_v2_blast_base_trades')
    , ref('sushiswap_v2_blast_base_trades')
    , ref('dackieswap_v3_blast_base_trades')
    , ref('swapblast_blast_base_trades')
    , ref('dyorswap_blast_base_trades')
    , ref('icecreamswap_v2_blast_base_trades')
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
        , blockchain = 'blast'
        , columns = ['from', 'to', 'index']
    )
}}
