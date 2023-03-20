{{ config(
        alias ='claims',
        post_hook='{{ expose_spells(\'["ethereum", "optimism"]\',
                                      "sector",
                                      "airdrop",
                                    \'["hildobby"]\') }}'
    )
}}


{% set airdrop_claims_models = [
    ref('airdrop_ethereum_claims')
    , ref('airdrop_optimism_claims')
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
    , quantity
    , token_address
    , token_symbol
    , evt_index
    FROM {{ airdrop_claims_model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)
