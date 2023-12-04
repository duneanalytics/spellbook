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
        , first_inscription_block = 
)}}
-- First inscription block is , : 