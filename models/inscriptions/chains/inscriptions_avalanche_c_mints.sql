{{ config(
        
        schema = 'inscriptions_avalanche_c',
        alias = 'sandwiches',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash']
)
}}

{{inscriptions_mints(
        blockchain='avalanche_c'
        , transactions = source('avalanche_c','transactions')
        , first_inscription_block = 31918263
)}}
-- First inscription block is 31918263, 2023-06-28 16:17: https://dune.com/queries/3254018