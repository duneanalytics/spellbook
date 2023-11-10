{{ config(
    schema = 'staking_ethereum',
    alias = 'entities_withdrawal_credentials',
    tags = ['static'],
    unique_key = ['withdrawal_credentials'])
}}

SELECT withdrawal_credentials, entity, entity_unique_name, category
FROM
(VALUES
(0x0000, '', '', 'CEX')
    ) 
    x (withdrawal_credentials, entity, entity_unique_name, category)