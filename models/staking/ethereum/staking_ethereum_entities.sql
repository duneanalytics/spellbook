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
     ('depositor_address', ref('staking_ethereum_entities_depositor_addresses'), 'main')
     , ('depositor_address', ref('staking_ethereum_entities_contracts'), 'main')
     , ('tx_from', ref('staking_ethereum_entities_tx_from_addresses'), 'main')
     , ('tx_from', ref('staking_ethereum_entities_batch_contracts_tx_from'), 'main')
     , ('pubkey', ref('staking_ethereum_entities_batch_contracts_pubkey'), 'main')
     , ('withdrawal_credentials', ref('staking_ethereum_entities_withdrawal_credentials'), 'main')

     , ('depositor_address', ref('staking_ethereum_entities_coinbase'), 'main')
     , ('depositor_address', ref('staking_ethereum_entities_binance'), 'main')
     , ('depositor_address', ref('staking_ethereum_entities_bitcoin_suisse'), 'main')
     , ('depositor_address', ref('staking_ethereum_entities_darma_capital'), 'main')
     , ('pubkey', ref('staking_ethereum_entities_chorusone'), 'sub')
] %}

SELECT DISTINCT depositor_address
, tx_from
, pubkey
, withdrawal_credentials
, entity
, entity_unique_name
, category
, entity FILTER (WHERE entity IS NOT NULL) AS entity
, entity_unique_name FILTER (WHERE entity_unique_name IS NOT NULL) AS entity_unique_name
, category FILTER (WHERE category IS NOT NULL) AS category
, sub_entity FILTER (WHERE sub_entity IS NOT NULL) AS sub_entity
, sub_entity_unique_name FILTER (WHERE sub_entity_unique_name IS NOT NULL) AS sub_entity_unique_name
, sub_entity_category FILTER (WHERE sub_entity_category IS NOT NULL) AS sub_entity_category
FROM (
        {% for entities_identifiers_model in entities_identifiers_models if entities_identifiers_model[0] == 'depositor_address' %}
        SELECT depositor_address
        , from_hex(NULL) AS tx_from
        , from_hex(NULL) AS pubkey
        , from_hex(NULL) AS withdrawal_credentials
        , CASE WHEN '{{ entities_identifiers_model[2] }}' = 'main' THEN entity ELSE NULL END AS entity
        , CASE WHEN '{{ entities_identifiers_model[2] }}' = 'main' THEN entity_unique_name ELSE NULL END AS entity_unique_name
        , CASE WHEN '{{ entities_identifiers_model[2] }}' = 'main' THEN category ELSE NULL END AS category
        , CASE WHEN '{{ entities_identifiers_model[2] }}' = 'sub' THEN entity ELSE NULL END AS sub_entity
        , CASE WHEN '{{ entities_identifiers_model[2] }}' = 'sub' THEN entity_unique_name ELSE NULL END AS sub_entity_unique_name
        , CASE WHEN '{{ entities_identifiers_model[2] }}' = 'sub' THEN category ELSE NULL END AS sub_entity_category
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
        , CASE WHEN '{{ entities_identifiers_model[2] }}' = 'main' THEN entity ELSE NULL END AS entity
        , CASE WHEN '{{ entities_identifiers_model[2] }}' = 'main' THEN entity_unique_name ELSE NULL END AS entity_unique_name
        , CASE WHEN '{{ entities_identifiers_model[2] }}' = 'main' THEN category ELSE NULL END AS category
        , CASE WHEN '{{ entities_identifiers_model[2] }}' = 'sub' THEN entity ELSE NULL END AS sub_entity
        , CASE WHEN '{{ entities_identifiers_model[2] }}' = 'sub' THEN entity_unique_name ELSE NULL END AS sub_entity_unique_name
        , CASE WHEN '{{ entities_identifiers_model[2] }}' = 'sub' THEN category ELSE NULL END AS sub_entity_category
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
        , CASE WHEN '{{ entities_identifiers_model[2] }}' = 'main' THEN entity ELSE NULL END AS entity
        , CASE WHEN '{{ entities_identifiers_model[2] }}' = 'main' THEN entity_unique_name ELSE NULL END AS entity_unique_name
        , CASE WHEN '{{ entities_identifiers_model[2] }}' = 'main' THEN category ELSE NULL END AS category
        , CASE WHEN '{{ entities_identifiers_model[2] }}' = 'sub' THEN entity ELSE NULL END AS sub_entity
        , CASE WHEN '{{ entities_identifiers_model[2] }}' = 'sub' THEN entity_unique_name ELSE NULL END AS sub_entity_unique_name
        , CASE WHEN '{{ entities_identifiers_model[2] }}' = 'sub' THEN category ELSE NULL END AS sub_entity_category
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
        , CASE WHEN '{{ entities_identifiers_model[2] }}' = 'main' THEN entity ELSE NULL END AS entity
        , CASE WHEN '{{ entities_identifiers_model[2] }}' = 'main' THEN entity_unique_name ELSE NULL END AS entity_unique_name
        , CASE WHEN '{{ entities_identifiers_model[2] }}' = 'main' THEN category ELSE NULL END AS category
        , CASE WHEN '{{ entities_identifiers_model[2] }}' = 'sub' THEN entity ELSE NULL END AS sub_entity
        , CASE WHEN '{{ entities_identifiers_model[2] }}' = 'sub' THEN entity_unique_name ELSE NULL END AS sub_entity_unique_name
        , CASE WHEN '{{ entities_identifiers_model[2] }}' = 'sub' THEN category ELSE NULL END AS sub_entity_category
        FROM {{ entities_identifiers_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        )