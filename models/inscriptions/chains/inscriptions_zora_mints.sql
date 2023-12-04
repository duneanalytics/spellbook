{{ config(
        
        schema = 'inscriptions_zora',
        alias = 'sandwiches',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain','tx_hash']
)
}}

{{inscriptions_mints(
        blockchain='zora'
        , transactions = source('zora','transactions')
        , first_inscription_block = 2137565
)}}
-- First inscription block is 2137565, 2023-08-02 09:36: https://dune.com/queries/3254037