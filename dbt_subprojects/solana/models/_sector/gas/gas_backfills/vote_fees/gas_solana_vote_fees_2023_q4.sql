{{ config(
    schema = 'gas_solana',
    alias = 'vote_fees_2023_q4',
    tags = ['static'],
    partition_by = ['block_date', 'block_hour'],
    materialized = 'table',
    file_format = 'delta'
) }}

{{ solana_vote_fees_macro(
    "cast('2023-10-01' as timestamp)",
    "cast('2024-01-01' as timestamp)"
) }}
