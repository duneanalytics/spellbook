{{
    config(
        schema='pyth_entropy',
        alias='pyth_entropy_request',
        post_hook='{{ expose_spells(\'["abstract", "apechain", "arbitrum", "b3", "base", "berachain", "blast", "kaia", "mode", "optimism", "sei", "sonic"]\',
                                  "project",
                                  "pyth",
                                  \'["gunboats"]\') }}'
    )
}}
{% set pyth_entropy_models = [
    ref('pyth_entropy_abstract_request')
    , ref('pyth_entropy_apechain_request')
    , ref('pyth_entropy_arbitrum_request')
    , ref('pyth_entropy_b3_request')
    , ref('pyth_entropy_base_request')
    , ref('pyth_entropy_berachain_request')
    , ref('pyth_entropy_blast_request')
    , ref('pyth_entropy_kaia_request')
    , ref('pyth_entropy_mode_request')
    , ref('pyth_entropy_optimism_request')
    , ref('pyth_entropy_sei_request')
    , ref('pyth_entropy_sonic_request')
] %}

SELECT *
FROM (
    {% for model in pyth_entropy_models %}
    SELECT
        blockchain
        , tx_hash
        , fee
        , assigned_sequence_number
        , symbol
        , provider
        , block_time
        , block_date
        , caller
    FROM {{ model }}
    {% if not loop.last %}
    UNION ALL
    {% endif %}
    {% endfor %}
)