{{ config(
    schema = 'dex_monad'
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
    ref('kuru_monad_base_trades')
    , ref('pinot_v2_monad_base_trades')
    , ref('pinot_v3_monad_base_trades')
    , ref('uniswap_v2_monad_base_trades')
    , ref('uniswap_v3_monad_base_trades')
    , ref('uniswap_v4_monad_base_trades')
    , ref('pancakeswap_v2_monad_base_trades')
    , ref('pancakeswap_v3_monad_base_trades')
] %}
with base_union as (
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

, add_tx_columns as (
    {{
        add_tx_columns(
            model_cte = 'base_union'
            , blockchain = 'monad'
            , columns = ['from', 'to', 'index']
        )
    }}
)
, final as (
    select
        *
        , row_number() over (partition by tx_hash, evt_index order by tx_hash) as duplicates_rank
    from
        add_tx_columns
)
select
    *
from
    final
where
    duplicates_rank = 1
