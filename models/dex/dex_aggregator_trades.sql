
{{ config(
        
        schema ='dex_aggregator',
        alias = 'trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
        incremental_predicates = ['DBT_INTERNAL_DEST.block_date >= date_trunc(\'day\', now() - interval \'7\' day)'],
        post_hook='{{ expose_spells(\'["ethereum", "gnosis", "avalanche_c", "fantom", "bnb", "optimism", "arbitrum"]\',
                                "sector",
                                "dex_aggregator",
                                \'["bh2smith", "Henrystats", "jeff-dude", "rantum" ]\') }}'
        )
}}

/********************************************************
spells with issues, to be excluded in short term:
-- ,ref('odos_trades') contains duplicates and not migrated to dunesql
********************************************************/

{% set dex_aggregator_models = [
    ref('cow_protocol_trades')
    ,ref('openocean_trades')
    ,ref('paraswap_trades')
    ,ref('lifi_trades')
    ,ref('yield_yak_trades')
    ,ref('bebop_trades')
    ,ref('dodo_aggregator_trades')
    ,ref('zeroex_trades')
    ,ref('kyberswap_aggregator_trades')
    ,ref('tokenlon_trades')
    ,ref('firebird_finance_optimism_trades')
    ,ref('oneinch_ar_trades')
    ,ref('unidex_optimism_trades')
    ,ref('odos_optimism_trades')
] %}

{% for aggregator_model in dex_aggregator_models %}
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
FROM {{ aggregator_model }}
{% if is_incremental() %}
WHERE block_date >= date_trunc('day', now() - interval '7' day)
{% endif %}
{% if not loop.last %}
UNION ALL
{% endif %}
{% endfor %}
