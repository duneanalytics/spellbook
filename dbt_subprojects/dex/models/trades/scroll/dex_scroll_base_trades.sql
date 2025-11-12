{{ config(
    schema = 'dex_scroll'
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
    ref('uniswap_v3_scroll_base_trades')
    , ref('sushiswap_v2_scroll_base_trades')
    , ref('zebra_scroll_base_trades')
    , ref('scrollswap_scroll_base_trades')
    , ref('syncswap_v1_scroll_base_trades')
    , ref('nuri_scroll_base_trades')
    , ref('izumi_finance_scroll_base_trades')
    , ref('icecreamswap_v2_scroll_base_trades')
    , ref('maverick_v2_scroll_base_trades')
    , ref('swaap_v2_scroll_base_trades')
    , ref('leetswap_scroll_base_trades')
    , ref('spacefi_scroll_base_trades')
    , ref('punkswap_scroll_base_trades')
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
        , blockchain = 'scroll'
        , columns = ['from', 'to', 'index']
    )
}}
