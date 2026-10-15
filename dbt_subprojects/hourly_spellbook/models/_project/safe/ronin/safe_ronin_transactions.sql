{{
    config(
        materialized='incremental',
        schema = 'safe_ronin',
        alias= 'transactions',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'tx_hash', 'trace_address'],
        file_format ='delta',
        incremental_strategy='merge'
        , post_hook='{{ hide_spells() }}'
    )
}}

{% set project_start_date = '2024-10-01' %}

{{ safe_transactions('ronin', project_start_date) }}
