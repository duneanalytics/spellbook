{{ config(
        
        schema = 'inscriptions_fantom',
        alias = 'sandwiches',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash']
)
}}

{{inscriptions_mints(
        blockchain='fantom'
        , transactions = source('fantom','transactions')
        , first_inscription_block = 64995274
)}}
-- First inscription block is 64995274, 2023-07-01 22:55: https://dune.com/queries/3254002