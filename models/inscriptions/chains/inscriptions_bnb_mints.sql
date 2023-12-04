{{ config(
        
        schema = 'inscriptions_bnb',
        alias = 'sandwiches',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash']
)
}}

{{inscriptions_mints(
        blockchain='bnb'
        , transactions = source('bnb','transactions')
        , first_inscription_block = 29257005
)}}
-- First inscription block is 29257005, 2023-06-20 04:37: https://dune.com/queries/3254013