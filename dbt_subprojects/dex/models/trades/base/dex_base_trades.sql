{{ config(
    schema = 'dex_base'
    , alias = 'trades'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

WITH balancer_v3 AS (
    -- due to Balancer V3 having trades between ERC4626 tokens, which won't be priced on prices.usd, enrich separately.
    {{
        enrich_balancer_v3_dex_trades(
            base_trades = ref('dex_base_base_trades')
            , filter = "(project = 'balancer' AND version = '3')"
            , tokens_erc20_model = source('tokens', 'erc20')
            , blockchain = 'base'
        )
    }}
)
, dexs AS (
    {{
        enrich_dex_trades(
            base_trades = ref('dex_base_base_trades')
            , filter = "NOT (project = 'balancer' AND version = '3')"
            , tokens_erc20_model = source('tokens', 'erc20')
            , blockchain = 'base'
        )
    }}
)

{% set cte_to_union = [
    'dexs'
    , 'balancer_v3'
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

