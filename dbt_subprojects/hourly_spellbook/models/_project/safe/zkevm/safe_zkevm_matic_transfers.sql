{{
    config(
        schema = 'safe_zkevm',
        alias = 'matic_transfers',
        partition_by = ['block_month'],
        on_schema_change='fail',
        materialized='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['block_date', 'address', 'tx_hash', 'trace_address'],
        post_hook = '{{ expose_spells(
                        blockchains = \'["zkevm"]\',
                        spell_type = "project",
                        spell_name = "safe",
                        contributors = \'["danielpartida"]\') }}'
    )
}}

{% set project_start_date = '2023-09-01' %}

{{
    safe_native_transfers(
        blockchain = 'zkevm',
        native_token_symbol = 'POL',
        project_start_date = project_start_date
    )
}}
