{{ config(
    alias = alias('entities'),
    tags = ['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['address'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "staking",
                                \'["hildobby", "sankinyue"]\') }}')
}}

{% set entities_addresses_models = [
     ('depositor_address', ref('staking_ethereum_entities_addresses'))
     , ('depositor_address', ref('staking_ethereum_entities_contracts'))
     , ('depositor_address', ref('staking_ethereum_entities_coinbase'))
     , ('depositor_address', ref('staking_ethereum_entities_binance'))
     , ('depositor_address', ref('staking_ethereum_entities_darma_capital'))
     , ('pubkey', ref('staking_ethereum_entities_chorusone'))
] %}

SELECT depositor_address
, tx_from
, pubkey
, entity
, entity_unique_name
, category
FROM (
        {% for entities_addresses_model in entities_addresses_models if entities_addresses_model[0] == 'depositor_address' %}
        SELECT depositor_address
        , from_hex(NULL) AS tx_from
        , from_hex(NULL) AS pubkey
        , entity
        , entity_unique_name
        , category
        FROM {{ entities_addresses_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}

        UNION ALL

        {% for entities_addresses_model in entities_addresses_models if entities_addresses_model[0] == 'pubkey' %}
        SELECT from_hex(NULL) AS depositor_address
        , from_hex(NULL) AS tx_from
        , pubkey AS pubkey
        , entity
        , entity_unique_name
        , category
        FROM {{ entities_addresses_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )