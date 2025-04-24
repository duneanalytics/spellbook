{{ config(
    schema = 'tokens_solana',
    alias = 'base_token22_spl_transfers_2024_q4',
    tags = ['static', 'prod_exclude'],
    partition_by = ['block_date'],
    materialized = 'table',
    file_format = 'delta'
) }}

{{ solana_token22_spl_transfers_macro(
    "cast('2024-10-01' as timestamp)",
    "cast('2025-01-01' as timestamp)"
) }} 