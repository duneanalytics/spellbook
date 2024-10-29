{{ config(
    schema = 'gas_solana',
    alias = 'vote_fees_2023_q1',
    materialized = 'view'
) }}

{{ solana_vote_fees_macro(
    "cast('2023-01-01' as timestamp)",
    "cast('2023-04-01' as timestamp)"
) }}
