{{ config(
        alias ='claims',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'evt_index', 'recipient'],
        post_hook='{{ expose_spells(\'["ethereum", "optimism", "arbitrum", "avalanche_c", "bnb", "gnosis"]\',
                                      "sector",
                                      "airdrop",
                                    \'["hildobby"]\') }}'
    )
}}


{% set airdrop_claims_models = [
    ref('airdrop_ethereum_claims')
    , ref('airdrop_optimism_claims')
    , ref('airdrop_arbitrum_claims')
    , ref('airdrop_avalanche_c_claims')
    , ref('airdrop_bnb_claims')
    , ref('airdrop_gnosis_claims')
] %}


SELECT *
FROM (
    {% for airdrop_claims_model in airdrop_claims_models %}
    SELECT
        blockchain
        , block_time
        , block_number
        , project
        , airdrop_identifier
        , recipient
        , contract_address
        , tx_hash
        , amount_raw
        , amount_original
        , amount_usd
        , token_address
        , token_symbol
        , evt_index
    FROM {{ airdrop_claims_model }}
    {% if is_incremental() %}
    WHERE block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
