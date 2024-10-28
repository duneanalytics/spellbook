{{ config(
    schema = 'gas_solana',
    alias = 'vote_fees_2024_q4',
    tags = ['static'],
    materialized = 'table',
    file_format = 'delta'
) }}

{{ solana_vote_fees_macro(
    "cast('2024-10-01' as timestamp)",
    "cast('current_date' as timestamp)"
) }}
