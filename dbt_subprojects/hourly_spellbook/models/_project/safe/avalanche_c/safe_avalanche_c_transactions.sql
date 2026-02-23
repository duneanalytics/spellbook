{{ 
    config(
        materialized='incremental',
        schema='safe_avalanche_c',
        alias = 'transactions',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'tx_hash', 'trace_address'], 
        file_format ='delta',
        incremental_strategy='merge'
        , post_hook='{{ hide_spells() }}'
    ) 
}}

{{ safe_transactions('avalanche_c', '2021-10-05') }}
