{{ config(
    schema ='dex_aggregator'
    , alias = 'base_trades'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set base_trade_models = [
    ref('lifi_base_trades')
    , ref('firebird_finance_optimism_base_trades')
    , ref('yield_yak_base_trades')
    , ref('unidex_optimism_base_trades')
] %}

with base_union as (
    SELECT *
    FROM
    (
        {% for model in base_trade_models %}
        SELECT
            blockchain
            , project
            , version
            , block_date
            , block_month
            , block_time
            -- , block_number -- missing yet
            , cast(token_bought_amount_raw as uint256) as token_bought_amount_raw
            , cast(token_sold_amount_raw as uint256) as token_sold_amount_raw
            , token_bought_address
            , token_sold_address
            , taker
            , maker
            , project_contract_address
            , tx_hash
            , tx_from
            , tx_to
            , trace_address
            , evt_index
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
)
select
    *
from
    base_union
