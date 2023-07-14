
{{ config(
        schema ='dex_aggregator',
        alias = alias('trades'),
        partition_by = ['block_date'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['block_date', 'blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'trace_address'],
        post_hook='{{ expose_spells(\'["ethereum", "gnosis", "avalanche_c", "fantom", "bnb", "optimism", "arbitrum"]\',
                                "sector",
                                "dex_aggregator",
                                \'["bh2smith", "Henrystats", "jeff-dude", "rantum" ]\') }}'
        )
}}

/********************************************************
spells with issues, to be excluded in short term:
-- ,ref('odos_trades') contains duplicates
********************************************************/

{% set dex_aggregator_models = [
 ref('cow_protocol_trades_legacy')
 ,ref('oneinch_ethereum_trades')
 ,ref('openocean_trades')
 ,ref('paraswap_trades')
 ,ref('lifi_trades')
 ,ref('yield_yak_avalanche_c_trades')
 ,ref('bebop_trades')
 ,ref('zeroex_trades')
 ,ref('dodo_aggregator_trades')
] %}

SELECT *
FROM (
    {% for aggregator_model in dex_aggregator_models %}
    SELECT
          blockchain
         , project
         , version
         , block_date
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
         , trace_address --ensure field is explicitly cast as array<bigint> in base models
         , evt_index
    FROM {{ aggregator_model }}
    {% if is_incremental() %}
    WHERE block_date >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
;