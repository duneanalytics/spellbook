{{ config(
    schema = 'tokens_solana',
    alias = 'sol_transfers_2022_q1',
    tags = ['static'],
    partition_by = ['block_date'],
    materialized = 'table',
    file_format = 'delta'
) }}

{{ solana_sol_transfers_macro(
    "cast('2022-01-01' as timestamp)",
    "cast('2022-04-01' as timestamp)"
) }} 