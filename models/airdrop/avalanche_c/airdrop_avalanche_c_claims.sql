{{ config(
        alias = alias('claims'),
        post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                      "sector",
                                      "airdrop",
                                    \'["hildobby"]\') }}'
    )
}}


{% set airdrop_claims_models = [
    ref('pangolin_avalanche_c_airdrop_claims')
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
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
