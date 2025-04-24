{{ config(
    schema = 'tokens_solana',
    alias = 'base_spl_transfers_call_transfer_2023_q4',
    tags = ['static', 'prod_exclude'],
    partition_by = ['block_date'],
    materialized = 'table',
    file_format = 'delta'
) }}

{{ solana_spl_transfers_call_transfer_macro(
    "cast('2023-10-01' as timestamp)",
    "cast('2024-01-01' as timestamp)"
) }} 