{{ config(
    schema = 'tokens_solana',
    alias = 'base_spl_transfers_call_transfer_2025_q1',
    tags = ['static', 'prod_exclude'],
    partition_by = ['block_date'],
    materialized = 'table',
    file_format = 'delta'
) }}

{{ solana_spl_transfers_call_transfer_macro(
    "cast('2025-01-01' as timestamp)",
    "cast('2025-04-01' as timestamp)"
) }} 