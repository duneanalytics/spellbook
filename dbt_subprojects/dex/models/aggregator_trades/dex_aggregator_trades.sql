{{ config(
        schema ='dex_aggregator',
        alias = 'trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
        incremental_predicates = ['DBT_INTERNAL_DEST.block_date >= date_trunc(\'day\', now() - interval \'7\' day)'],
        post_hook='{{ expose_spells(\'["ethereum", "gnosis", "avalanche_c", "fantom", "bnb", "optimism", "arbitrum", "base", "linea", "scroll", "polygon"]\',
                                "sector",
                                "dex_aggregator",
                                \'["bh2smith", "Henrystats", "jeff-dude", "rantum", "hosuke"]\') }}'
        )
}}

{% set as_is_models = [
    ref('cow_protocol_trades')
    ,ref('paraswap_trades')
    ,ref('yield_yak_trades')
    ,ref('bebop_trades')
    ,ref('dodo_aggregator_trades')
    ,ref('zeroex_trades')
    ,ref('kyberswap_aggregator_trades')
    ,ref('tokenlon_trades')
    ,ref('firebird_finance_optimism_trades')
    ,ref('oneinch_ar_trades')
    ,ref('unidex_optimism_trades')
    ,ref('odos_trades')
] %}

WITH enriched_aggregator_base_trades AS (
    {{
        enrich_dex_aggregator_trades(
            base_trades = ref('dex_aggregator_base_trades')
            , filter = "1 = 1"
            , tokens_erc20_model = source('tokens', 'erc20')
        )
    }}
)

, as_is_dexs AS (
    {% for model in as_is_models %}
    SELECT
        blockchain
        , project
        , version
        , block_date
        , block_month
        , block_time
        , token_bought_symbol
        , token_sold_symbol
        , token_pair
        , token_bought_amount
        , token_sold_amount
        , cast(token_bought_amount_raw as uint256) as token_bought_amount_raw
        , cast(token_sold_amount_raw as uint256) as token_sold_amount_raw
        , amount_usd
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
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('block_time') }}
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
{% set cte_to_union = [
    'enriched_aggregator_base_trades'
    , 'as_is_dexs'
    ]
%}

{% for cte in cte_to_union %}
    SELECT
        blockchain
        , project
        , version
        , block_date
        , block_month
        , block_time
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
        , trace_address
        , evt_index
    FROM
        {{ cte }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
{% endfor %}
