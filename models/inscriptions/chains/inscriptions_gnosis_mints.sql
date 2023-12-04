{{ config(
        
        schema = 'inscriptions_gnosis',
        alias = 'sandwiches',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash']
)
}}

{{inscriptions_mints(
        blockchain='gnosis'
        , transactions = source('gnosis','transactions')
        , first_inscription_block = 28732751
)}}
-- First inscription block is 28732751, 2023-07-01 16:09: https://dune.com/queries/3253998