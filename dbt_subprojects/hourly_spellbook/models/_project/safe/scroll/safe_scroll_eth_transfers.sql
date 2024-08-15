{{
    config(
        schema = 'safe_scroll',
        alias= 'eth_transfers',
        partition_by = ['block_month'],
        on_schema_change='fail',
        materialized='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['block_date', 'address', 'tx_hash', 'trace_address'],
        post_hook = '{{ expose_spells(blockchains = \'["scroll"]\',
                                    spell_type = "project",
                                    spell_name = "safe",
                                    contributors = \'["danielpartida"]\') }}'
    )
}}

{% set project_start_date = '2023-10-15' %}

{{
    safe_native_transfers(
        blockchain = 'scroll',
        native_token_symbol = 'ETH',
        project_start_date = project_start_date
    )
}}