{{ config(
    schema = 'staking_ethereum',
    alias = alias('entities'),
    tags = ['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['depositor_address', 'tx_from', 'pubkey'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "staking",
                                \'["hildobby", "sankinyue"]\') }}')
}}

{% set entities_depositor_address_models = [
     (ref('staking_ethereum_entities_depositor_addresses'))
     , (ref('staking_ethereum_entities_contracts'))
     , (ref('staking_ethereum_entities_coinbase'))
     , (ref('staking_ethereum_entities_binance'))
     , (ref('staking_ethereum_entities_darma_capital'))
] %}

{% set entities_tx_from_models = [
     (ref('staking_ethereum_entities_tx_from_addresses'))
] %}

{% set entities_pubkey_models = [
     (ref('staking_ethereum_entities_chorusone'))
] %}

SELECT depositor_address
, tx_from
, pubkey
, entity
, entity_unique_name
, category
FROM (
        {% for entities_depositor_address_model in entities_depositor_address_models %}
        SELECT depositor_address
        , from_hex(NULL) AS tx_from
        , from_hex(NULL) AS pubkey
        , entity
        , entity_unique_name
        , category
        FROM {{ entities_depositor_address_model }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}

        UNION ALL

        {% for entities_tx_from_model in entities_tx_from_models %}
        SELECT from_hex(NULL) AS depositor_address
        , tx_from
        , from_hex(NULL) AS pubkey
        , entity
        , entity_unique_name
        , category
        FROM {{ entities_tx_from_model }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}

        UNION ALL

        {% for entities_pubkey_model in entities_pubkey_models %}
        SELECT from_hex(NULL) AS depositor_address
        , from_hex(NULL) AS tx_from
        , pubkey AS pubkey
        , entity
        , entity_unique_name
        , category
        FROM {{ entities_pubkey_model }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )