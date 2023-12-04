{{ config(
        
        schema = 'inscriptions_optimism',
        alias = 'sandwiches',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash']
)
}}

{{inscriptions_mints(
        blockchain='optimism'
        , transactions = source('optimism','transactions')
)}}
