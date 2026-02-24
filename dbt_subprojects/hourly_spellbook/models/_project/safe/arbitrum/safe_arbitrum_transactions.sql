{{ 
    config(
        materialized='incremental',
        schema='safe_arbitrum',
        alias = 'transactions',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'tx_hash', 'trace_address'], 
        file_format ='delta',
        incremental_strategy='merge'
        , post_hook='{{ hide_spells() }}'
    ) 
}}

{{ safe_transactions('arbitrum', '2021-06-20') }}
