{{ config(
    schema = 'tokens_solana',
    alias = 'sol_transfers_2022_q3',
    tags = ['prod_exclude'],
    partition_by = ['block_date'],
    materialized = 'table',
    file_format = 'delta'
) }}

{{ solana_sol_transfers_macro(
    "cast('2022-07-01' as timestamp)",
    "cast('2022-10-01' as timestamp)"
) }} 