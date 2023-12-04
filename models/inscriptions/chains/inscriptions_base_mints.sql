{{ config(
        
        schema = 'inscriptions_base',
        alias = 'sandwiches',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash']
)
}}

{{inscriptions_mints(
        blockchain='base'
        , transactions = source('base','transactions')
        , first_inscription_block = 2041131
)}}
-- First inscription block is 2041131, 2023-08-01 06:33: https://dune.com/queries/3254019