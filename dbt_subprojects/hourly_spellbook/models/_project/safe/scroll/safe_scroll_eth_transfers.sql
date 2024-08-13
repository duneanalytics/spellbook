{{
    config(
        materialized='incremental',
        schema = 'safe_scroll',
        alias= 'eth_transfers',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'address', 'tx_hash', 'trace_address'],
        on_schema_change='fail',
        file_format ='delta',
        incremental_strategy='merge',
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