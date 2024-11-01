{{ config(
    schema = 'gas_solana',
    alias = 'vote_fees_2024_q3',
    materialized = 'view'
) }}

{{ solana_vote_fees_macro(
    "cast('2024-07-01' as timestamp)",
    "cast('2024-10-01' as timestamp)"
) }}
