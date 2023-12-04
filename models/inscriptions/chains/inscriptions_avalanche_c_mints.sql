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
)}}
