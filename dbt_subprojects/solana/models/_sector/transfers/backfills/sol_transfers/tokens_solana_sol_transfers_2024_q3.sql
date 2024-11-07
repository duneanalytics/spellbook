{{ config(
    schema = 'tokens_solana',
    alias = 'sol_transfers_2024_q3',
    tags = ['prod_exclude'],
    partition_by = ['block_date'],
    materialized = 'table',
    file_format = 'delta'
) }}

{{ solana_sol_transfers_macro(
    "cast('2024-07-01' as timestamp)",
    "cast('2024-10-01' as timestamp)"
) }} 