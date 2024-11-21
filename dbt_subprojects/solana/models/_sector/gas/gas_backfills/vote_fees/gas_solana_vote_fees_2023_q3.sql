{{ config(
    schema = 'gas_solana',
    alias = 'vote_fees_2023_q3',
    materialized = 'view'
) }}

{{ solana_vote_fees_macro(
    "cast('2023-07-01' as timestamp)",
    "cast('2023-10-01' as timestamp)"
) }}
