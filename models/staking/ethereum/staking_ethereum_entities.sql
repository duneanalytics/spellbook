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

SELECT *
FROM (
        {% for entities_addresses_model in entities_addresses_models %}
        SELECT CASE '{{ entities_addresses_model[0] }}'='depositor_address' THEN depositor_address ELSE NULL END AS deposit_address
        SELECT CASE '{{ entities_addresses_model[0] }}'='tx_from' THEN tx_from ELSE NULL END AS tx_from
        SELECT CASE '{{ entities_addresses_model[0] }}'='pubkey' THEN pubkey ELSE NULL END AS pubkey
        , entity
        , entity_unique_name
        , category
        FROM {{ entities_addresses_model[1] }}
        {% if not loop.last %}
        UNION ALL
        {% endif %}
        {% endfor %}
        );