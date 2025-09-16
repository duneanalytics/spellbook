{{
    config(
        materialized='incremental',
        schema = 'safe_berachain',
        alias= 'transactions',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'tx_hash', 'trace_address'],
        file_format ='delta',
        incremental_strategy='merge',
        post_hook = '{{ expose_spells(
                        blockchains = \'["berachain"]\',
                        spell_type = "project",
                        spell_name = "safe",
                        contributors = \'["petertherock"]\') }}'
    )
}}

{% set project_start_date = '2025-01-01' %}

{{ safe_transactions('berachain', project_start_date) }}
