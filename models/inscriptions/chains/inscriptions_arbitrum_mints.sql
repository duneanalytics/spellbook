{{ config(
        
        schema = 'inscriptions_arbitrum',
        alias = 'sandwiches',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain','tx_hash']
)
}}

{{inscriptions_mints(
        blockchain='arbitrum'
        , transactions = source('arbitrum','transactions')
        , first_inscription_block = 102376455
)}}
-- First inscription block is 102376455, 2023-06-18 09:35: https://dune.com/queries/3253988