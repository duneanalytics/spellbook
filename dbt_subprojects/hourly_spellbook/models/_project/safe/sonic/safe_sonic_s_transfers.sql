{{
    config(
        schema = 'safe_sonic',
        alias= 's_transfers',
        partition_by = ['block_month'],
        on_schema_change='fail',
        materialized='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['block_date', 'address', 'tx_hash', 'trace_address'],
        post_hook = '{{ expose_spells(blockchains = \'["sonic"]\',
                                    spell_type = "project",
                                    spell_name = "safe",
                                    contributors = \'["danielpartida"]\') }}'
    )
}}

{% set project_start_date = '2024-12-01' %}

{{
    safe_native_transfers(
        blockchain = 'sonic',
        native_token_symbol = 'S',
        project_start_date = project_start_date
    )
}}