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
     , ('withdrawal_credentials', ref('staking_ethereum_entities_withdrawal_credentials'), 'main')
     , ('tx_from', ref('staking_ethereum_entities_batch_contracts_tx_from'), 'sub')
     , ('pubkey', ref('staking_ethereum_entities_batch_contracts_pubkey'), 'sub')


     , ('depositor_address', ref('staking_ethereum_entities_coinbase'), 'main')
     , ('depositor_address', ref('staking_ethereum_entities_binance'), 'main')
     , ('depositor_address', ref('staking_ethereum_entities_bitcoin_suisse'), 'main')
     , ('depositor_address', ref('staking_ethereum_entities_darma_capital'), 'main')
     , ('depositor_address', ref('staking_ethereum_entities_stakewise_v3'), 'main')
     , ('pubkey', ref('staking_ethereum_entities_chorusone'), 'sub')
] %}

WITH entries AS (
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
     
, merged AS (
     SELECT e.depositor_address
     , e.tx_from
     , e.pubkey
     , e.withdrawal_credentials
     , e.entity
     , e.entity_unique_name
     , e.category
     , s.sub_entity
     , s.sub_entity_unique_name
     , s.sub_entity_category
     FROM entries e
     LEFT JOIN entries s ON e.depositor_address=s.depositor_address
          AND e.tx_from=s.tx_from
          AND e.pubkey=s.pubkey
          AND e.withdrawal_credentials=s.withdrawal_credentials
          AND s.sub_entity IS NOT NULL
     WHERE e.entity IS NOT NULL
     )

SELECT depositor_address
, tx_from
, pubkey
, withdrawal_credentials
, entity
, entity_unique_name
, category
, sub_entity
, sub_entity_unique_name
, sub_entity_category
FROM merged

UNION ALL

SELECT e.depositor_address
, e.tx_from
, e.pubkey
, e.withdrawal_credentials
, e.entity
, e.entity_unique_name
, e.category
, e.sub_entity
, e.sub_entity_unique_name
, e.sub_entity_category
FROM entries e
LEFT JOIN merged m ON m.depositor_address=e.depositor_address
     AND m.tx_from=e.tx_from
     AND m.pubkey=e.pubkey
     AND m.withdrawal_credentials=e.withdrawal_credentials
     AND m.entity IS NULL
WHERE e.sub_entity IS NOT NULL
