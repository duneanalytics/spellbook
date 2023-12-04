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
)}}
