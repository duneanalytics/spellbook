{{ config(
    schema = 'tokens_solana',
    alias = 'spl_transfers_call_transfer_current',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'append',
    unique_key = ['block_date', 'unique_instruction_key']
) }}

{{ solana_spl_transfers_call_transfer_macro(
    "cast('2025-04-01' as timestamp)",
    "now()"
) }} 