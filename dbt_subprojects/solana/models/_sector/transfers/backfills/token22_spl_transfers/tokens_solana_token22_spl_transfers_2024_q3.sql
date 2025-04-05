{{ config(
    schema = 'tokens_solana',
    alias = 'token22_spl_transfers_2024_q3',
    tags = ['static'],
    partition_by = ['block_date'],
    materialized = 'table',
    file_format = 'delta'
) }}

{{ solana_token22_spl_transfers_macro(
    "cast('2024-07-01' as timestamp)",
    "cast('2024-10-01' as timestamp)"
) }} 