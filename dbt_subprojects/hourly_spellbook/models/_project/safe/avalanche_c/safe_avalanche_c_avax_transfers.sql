{{ 
    config(
        schema = 'safe_avalanche_c',
        alias = 'avax_transfers',
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
        blockchain = 'avalanche_c'
        , native_token_symbol = 'AVAX'
        , project_start_date = '2021-10-05'
    )
}}
