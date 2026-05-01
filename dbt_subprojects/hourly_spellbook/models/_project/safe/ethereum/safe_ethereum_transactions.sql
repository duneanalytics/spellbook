{{ 
    config(
        materialized='incremental',
        schema='safe_ethereum',
        alias = 'transactions',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'tx_hash', 'trace_address'],
        file_format ='delta',
        incremental_strategy='merge'
        , post_hook='{{ hide_spells() }}'
    )
}}

{{ safe_transactions('ethereum', '2018-11-24') }}
