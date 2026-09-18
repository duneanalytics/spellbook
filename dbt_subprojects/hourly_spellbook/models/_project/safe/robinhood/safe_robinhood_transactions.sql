{{
    config(
        materialized='incremental',
        schema='safe_robinhood',
        alias= 'transactions',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'tx_hash', 'trace_address'],
        file_format ='delta',
        incremental_strategy='merge'
        , post_hook='{{ hide_spells() }}'
    )
}}

{{ safe_transactions('robinhood', '2023-07-01') }}
