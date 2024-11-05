{{ config(
    schema = 'tokens_solana',
    alias = 'spl_transfers_call_transfer_current',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_date', 'tx_id', 'outer_instruction_index', 'inner_instruction_index', 'block_slot']
) }}

{{ solana_spl_transfers_call_transfer_macro(
    "cast('2024-10-01' as timestamp)",
    "now()"
) }} 