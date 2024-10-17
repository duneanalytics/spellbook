{{ config(
        
        schema='attacks',
        alias = 'all',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'evt_index'],
        post_hook = '{{ expose_spells(
                        blockchains = \'["arbitrum"]\',
                        spell_type = "sector",
                        spell_name = "attacks",
                        contributors = \'["hildobby"]\') }}'
        )
}}

{% set attack_models = [
     (ref('attacks_address_poisoning'))
] %}

SELECT *
FROM (
        {% for attack_model in attack_models %}
        SELECT blockchain
        , block_time
        , block_number
        , attacker
        , victim
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