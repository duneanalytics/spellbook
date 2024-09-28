{% set blockchain = 'gnosis' %}

{{ config(
        
        schema = 'dex_' + blockchain,
        alias = 'sandwiches',
        partition_by = ['block_month'],
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['blockchain', 'tx_hash', 'evt_index']
)
}}

{{dex_sandwiches(
        blockchain = blockchain
        , transactions = source(blockchain,'transactions')
)}}
