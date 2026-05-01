{{
    config(
        materialized='incremental',
        schema = 'safe_mantle',
        alias= 'transactions',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'tx_hash', 'trace_address'],
        file_format ='delta',
        incremental_strategy='merge'
        , post_hook='{{ hide_spells() }}'
    )
}}

{% set project_start_date = '2023-07-12' %}

{{ safe_transactions('mantle', project_start_date) }}
