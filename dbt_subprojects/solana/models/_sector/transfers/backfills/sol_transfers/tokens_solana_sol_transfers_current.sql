{{ config(
    schema = 'tokens_solana',
    alias = 'sol_transfers_current',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'delete+insert',
    unique_key = ['tx_id', 'outer_instruction_index', 'inner_instruction_index', 'block_slot']
) }}

{{ solana_sol_transfers_macro(
    "cast('2024-10-01' as timestamp)",
    "now()"
) }} 