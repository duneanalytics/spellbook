{{ config(
    schema = 'gas_solana',
    alias = 'vote_fees_2024_q2',
    materialized = 'view'
) }}

{{ solana_vote_fees_macro(
    "cast('2024-04-01' as timestamp)",
    "cast('2024-07-01' as timestamp)"
) }}
