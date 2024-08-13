{{
    config(
        materialized='incremental',
        schema = 'safe_blast',
        alias = 'eth_transfers',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'address', 'tx_hash', 'trace_address'],
        on_schema_change='fail',
        file_format ='delta',
        incremental_strategy='merge',
        post_hook='{{ expose_spells(blockchains = \'["blast"]\',
                                    spell_type = "project",
                                    spell_name = "safe",
                                    contributors = \'["danielpartida", "hosuke"]\') }}'
    )
}}

{{
    safe_native_transfers(
        blockchain = 'blast'
        , native_token_symbol = 'ETH'
        , project_start_date = '2024-02-24'
    )
}}
