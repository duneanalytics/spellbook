{{
    config(
        materialized='incremental',
        schema = 'safe_blast',
        alias= 'transactions',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'tx_hash', 'trace_address'],
        file_format ='delta',
        incremental_strategy='merge',
        tags=['static'],
        post_hook='{{ hide_spells() }}'
    )
}}

{% set project_start_date = '2024-02-24' %}

{{ safe_transactions('blast', project_start_date) }}