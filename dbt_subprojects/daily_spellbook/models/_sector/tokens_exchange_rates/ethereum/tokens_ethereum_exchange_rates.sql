{{ config(
        schema = 'tokens_ethereum',
        alias ='exchange_rates',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
        unique_key = ['blockchain', 'tx_hash', 'evt_index'],
        post_hook='{{ expose_spells(blockchains = \'["ethereum"]\',
                                    spell_type = "sector",
                                    spell_name = "tokens",
                                    contributors = \'["Henrystats"]\') }}'
)
}}

{% set exchange_rate_models = [
 ref('tokens_ethereum_wsteth_exchange_rates')
] %}

SELECT *
FROM (
    {% for exchange_rate_model in exchange_rate_models %}
    SELECT blockchain
    , block_time
    , block_number
    , tx_hash
    , evt_index
    , project
    , token_address
    , token_symbol
    , token_type
    , underlying_token_address
    , underlying_token_symbol
    , rate
    , price_usd
    FROM {{ exchange_rate_model }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('block_time')}}
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)