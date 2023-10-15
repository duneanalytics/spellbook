{{ config(
    schema = 'staking_ethereum',
    alias = alias('entities_withdrawal_credentials'),
    tags = ['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['depositor_address'])
}}

SELECT from_hex(NULL) AS withdrawal_credentials
, CAST(NULL AS varchar) AS entity_unique_name
, CAST(NULL AS varchar) AS category


SELECT withdrawal_credentials, entity, entity_unique_name, category
FROM
(VALUES
(0x010000000000000000000000d1026749530a15c20cb7b30368d8c15e200fe1d6, 'Bitcoin Suisse', 'Bitcoin Suisse 13', 'CEX')
    ) 
    x (depositor_address, entity, entity_unique_name, category)