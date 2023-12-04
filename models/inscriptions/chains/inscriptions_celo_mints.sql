{{ config(
        
        schema = 'inscriptions_celo',
        alias = 'sandwiches',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain','tx_hash']
)
}}

{{inscriptions_mints(
        blockchain='celo'
        , transactions = source('celo','transactions')
        , first_inscription_block = 20374169
)}}
-- First inscription block is 20374169, 2023-07-16 20:25: https://dune.com/queries/3254010