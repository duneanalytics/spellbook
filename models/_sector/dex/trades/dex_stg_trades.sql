{{ config(
    schema = 'dex'
    , alias = 'stg_trades'
    , partition_by = ['block_month', 'blockchain', 'project']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    )
}}

{% set models = [
    ref('dex_arbitrum_stg_trades')
    , ref('dex_base_stg_trades')
    , ref('dex_bnb_stg_trades')
    , ref('dex_celo_stg_trades')
    , ref('dex_ethereum_stg_trades')
    , ref('dex_optimism_stg_trades')
    , ref('dex_polygon_stg_trades')
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
            , row_number() over (partition by tx_hash, evt_index order by tx_hash) as duplicates_rank
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
    WHERE
        duplicates_rank = 1
)
select
    *
from
    base_union