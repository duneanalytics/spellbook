{{
    config(
        materialized='table',
        schema = 'safe_blast',
        alias= 'transactions',
        partition_by = ['block_month'],
        file_format ='delta',
        tags=['static'],
        post_hook='{{ hide_spells() }}'
    )
}}

{% set project_start_date = '2024-02-24' %}

{{ safe_transactions('blast', project_start_date) }}