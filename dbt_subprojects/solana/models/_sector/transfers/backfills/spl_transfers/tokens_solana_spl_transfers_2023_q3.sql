{{ config(
    schema = 'tokens_solana',
    alias = 'spl_transfers_2023_q3',
    tags = ['static'],
    partition_by = ['block_date'],
    materialized = 'table',
    file_format = 'delta'
) }}

{{ solana_spl_transfers_macro(
    "cast('2023-07-01' as timestamp)",
    "cast('2023-10-01' as timestamp)"
) }} 