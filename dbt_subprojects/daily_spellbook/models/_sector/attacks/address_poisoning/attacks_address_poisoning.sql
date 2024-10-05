{{ config(
        schema = 'attacks',
        alias = 'address_poisoning',
        materialized = 'view',
        post_hook = '{{ expose_spells(
                        blockchains = \'["arbitrum"]\',
                        spell_type = "sector",
                        spell_name = "attacks",
                        contributors = \'["hildobby"]\') }}'
        )
}}

{% set attack_models = [
     (ref('attacks_arbitrum_address_poisoning'))
] %}

SELECT *
FROM (
        {% for attack_model in attack_models %}
        SELECT blockchain
        , block_time
        , block_number
        , attacker
        , victim
        , intended_recipient
        , amount_usd
        , amount
        , amount_raw
        , token_standard
        , token_address
        , token_symbol
        , tx_hash
        , tx_index
        , evt_index
        FROM {{ attack_model }}
        {% if is_incremental() %}
        WHERE {{ incremental_predicate('block_time') }}
        {% endif %}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )