{{
    config(
        materialized='incremental',
        schema = 'safe_linea',
        alias= 'eth_transfers',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'address', 'tx_hash', 'trace_address'],
        on_schema_change='fail',
        file_format ='delta',
        incremental_strategy='merge',
        post_hook = '{{ expose_spells(
                        blockchains = \'["linea"]\',
                        spell_type = "project",
                        spell_name = "safe",
                        contributors = \'["danielpartida"]\') }}'
    )
}}

{% set project_start_date = '2023-07-11' %}

{{
    safe_native_transfers(
        blockchain = 'linea',
        native_token_symbol = 'ETH',
        project_start_date = project_start_date
    )
}}
