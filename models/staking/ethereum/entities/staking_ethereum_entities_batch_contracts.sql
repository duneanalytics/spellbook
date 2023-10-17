{{ config(
    schema = 'staking_ethereum',
    alias = alias('entities_batch_contracts'),
    tags = ['dunesql'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['depositor_address'])
}}

SELECT pubkey, entity, entity_unique_name, category
FROM
(VALUES
(0x0000000000000000000000000000000000000000, '', '', 'CEX')
    ) 
    x (pubkey, entity, entity_unique_name, category)