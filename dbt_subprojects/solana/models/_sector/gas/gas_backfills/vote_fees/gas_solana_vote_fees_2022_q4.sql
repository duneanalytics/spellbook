{{ config(
    schema = 'gas_solana',
    alias = 'vote_fees_2022_q4',
    materialized = 'view'
) }}

{{ solana_vote_fees_macro(
    "cast('2022-10-01' as timestamp)",
    "cast('2023-01-01' as timestamp)"
) }}
