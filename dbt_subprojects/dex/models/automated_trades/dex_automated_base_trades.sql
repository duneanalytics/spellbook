{{ config(
    schema = 'dex'
    , alias = 'automated_base_trades'
    , partition_by = ['block_month', 'blockchain']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'block_number', 'index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

{% set models = [
    ref('dex_multichain_automated_base_trades')
] %}

with base_union as (
    SELECT *
    FROM
    (
        {% for model in models %}
        SELECT
            blockchain
            , version
            , factory_address
            , dex_type
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
