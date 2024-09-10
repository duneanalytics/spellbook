{{
    config(
        materialized='incremental',
        schema = 'safe_scroll',
        alias= 'transactions',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'tx_hash', 'trace_address'],
        file_format ='delta',
        incremental_strategy='merge',
        post_hook = '{{ expose_spells(
                        blockchains = \'["scroll"]\',
                        spell_type = "project",
                        spell_name = "safe",
                        contributors = \'["danielpartida"]\') }}'
    )
}}

{% set project_start_date = '2023-10-15' %}

{{ safe_transactions('scroll', project_start_date) }}
