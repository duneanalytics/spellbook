{{ config(
    schema = 'staking_ethereum',
    alias = 'entities',
    
    unique_key = ['depositor_address', 'tx_from', 'pubkey'],
    post_hook='{{ expose_spells(\'["ethereum"]\',
                                "sector",
                                "staking",
                                \'["hildobby", "sankinyue", "nerolation"]\') }}')
}}

{% set entities_identifiers_models = [
     ('depositor_address', ref('staking_ethereum_entities_depositor_addresses'))
     , ('depositor_address', ref('staking_ethereum_entities_contracts'))
     , ('depositor_address', ref('staking_ethereum_entities_coinbase'))
     , ('depositor_address', ref('staking_ethereum_entities_binance'))
     , ('depositor_address', ref('staking_ethereum_entities_darma_capital'))
     , ('tx_from', ref('staking_ethereum_entities_tx_from_addresses'))
     , ('pubkey', ref('staking_ethereum_entities_chorusone'))
     , ('tx_from', ref('staking_ethereum_entities_batch_contracts_tx_from'))
     , ('pubkey', ref('staking_ethereum_entities_batch_contracts_pubkey'))
     , ('withdrawal_credentials', ref('staking_ethereum_entities_withdrawal_credentials'))
     , ('depositor_address', ref('staking_ethereum_entities_bitcoin_suisse'))
] %}

SELECT depositor_address
, tx_from
, pubkey
, withdrawal_credentials
, entity
, entity_unique_name
, category
FROM (
        {% for entities_identifiers_model in entities_identifiers_models if entities_identifiers_model[0] == 'depositor_address' %}
        SELECT depositor_address
        , from_hex(NULL) AS tx_from
        , from_hex(NULL) AS pubkey
        , from_hex(NULL) AS withdrawal_credentials
        , entity
        , entity_unique_name
        , category
        FROM {{ entities_identifiers_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}

        UNION ALL

        {% for entities_identifiers_model in entities_identifiers_models if entities_identifiers_model[0] == 'tx_from' %}
        SELECT from_hex(NULL) AS depositor_address
        , tx_from
        , from_hex(NULL) AS pubkey
        , from_hex(NULL) AS withdrawal_credentials
        , entity
        , entity_unique_name
        , category
        FROM {{ entities_identifiers_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}

        UNION ALL

        {% for entities_identifiers_model in entities_identifiers_models if entities_identifiers_model[0] == 'pubkey' %}
        SELECT from_hex(NULL) AS depositor_address
        , from_hex(NULL) AS tx_from
        , pubkey
        , from_hex(NULL) AS withdrawal_credentials
        , entity
        , entity_unique_name
        , category
        FROM {{ entities_identifiers_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}

        UNION ALL

        {% for entities_identifiers_model in entities_identifiers_models if entities_identifiers_model[0] == 'withdrawal_credentials' %}
        SELECT from_hex(NULL) AS depositor_address
        , from_hex(NULL) AS tx_from
        , from_hex(NULL) AS pubkey
        , withdrawal_credentials
        , entity
        , entity_unique_name
        , category
        FROM {{ entities_identifiers_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )