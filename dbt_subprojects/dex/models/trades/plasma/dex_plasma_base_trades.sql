{{ config(
    schema = 'dex_plasma'
    , alias = 'base_trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set plasma_models = [
    ref('uniswap_v3_plasma_base_trades')
    , ref('fluid_v1_plasma_base_trades')
] %}

with plasma_union as (
    SELECT *
    FROM
    (
        {% for plasma_model in plasma_models %}
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
            {{ plasma_model }}
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
        model_cte = 'plasma_union'
        , blockchain = 'plasma'
        , columns = ['from', 'to', 'index']
    )
}} 