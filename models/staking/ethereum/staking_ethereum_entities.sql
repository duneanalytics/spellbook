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

SELECT address, entity, entity_unique_name, category FROM {{ ref('staking_ethereum_entities_addresses') }}
UNION ALL
SELECT address, entity, entity_unique_name, category FROM {{ ref('staking_ethereum_entities_contracts') }}
UNION ALL
SELECT address, entity, entity_unique_name, category FROM {{ ref('staking_ethereum_entities_coinbase') }}
UNION ALL
SELECT address, entity, entity_unique_name, category FROM {{ ref('staking_ethereum_entities_binance') }}
UNION ALL
SELECT address, entity, entity_unique_name, category FROM {{ ref('staking_ethereum_entities_darma_capital') }}