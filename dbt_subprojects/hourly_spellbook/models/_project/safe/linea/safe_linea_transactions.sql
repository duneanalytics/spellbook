{{
    config(
        materialized='incremental',
        schema = 'safe_linea',
        alias= 'transactions',
        partition_by = ['block_month'],
        unique_key = ['block_date', 'tx_hash', 'trace_address'],
        file_format ='delta',
        incremental_strategy='merge',
        post_hook = '{{ expose_spells(
                        blockchains = \'["linea"]\',
                        spell_type = "project",
                        spell_name = "safe",
                        contributors = \'["danielpartida"]\') }}'
    )
}}

{{ safe_transactions('linea', '2023-07-11') }}
