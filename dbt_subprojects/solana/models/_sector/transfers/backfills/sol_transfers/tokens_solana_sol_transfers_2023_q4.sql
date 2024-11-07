{{ config(
    schema = 'tokens_solana',
    alias = 'sol_transfers_2023_q4',
    tags = ['prod_exclude'],
    partition_by = ['block_date'],
    materialized = 'table',
    file_format = 'delta'
) }}

{{ solana_sol_transfers_macro(
    "cast('2023-10-01' as timestamp)",
    "cast('2024-01-01' as timestamp)"
) }} 