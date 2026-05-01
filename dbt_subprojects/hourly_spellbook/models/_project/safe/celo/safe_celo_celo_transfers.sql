{{
    config(
        schema = 'safe_celo',
        alias = 'celo_transfers',
        partition_by = ['block_month'],
        on_schema_change='fail',
        materialized='incremental',
        file_format ='delta',
        incremental_strategy='merge',
        unique_key = ['block_date', 'address', 'tx_hash', 'trace_address']
        , post_hook='{{ hide_spells() }}'
    )
}}

{{
    safe_native_transfers(
        blockchain = 'celo'
        , native_token_symbol = 'CELO'
        , project_start_date = '2021-06-20'
    )
}}
