{{ config(
    schema = 'staking_ethereum',
    alias = alias('entities_batch_contracts'),
    tags = ['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['depositor_address'])
}}

SELECT from_hex(NULL) AS pubkey
, CAST(NULL AS varchar) AS entity_unique_name
, CAST(NULL AS varchar) AS category