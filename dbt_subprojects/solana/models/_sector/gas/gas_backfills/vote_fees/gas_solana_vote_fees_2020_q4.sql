{{ config(
    schema = 'gas_solana',
    alias = 'vote_fees_2020_q4',
    materialized = 'view'
) }}

{{ solana_vote_fees_macro(
    "cast('2020-10-01' as timestamp)",
    "cast('2021-01-01' as timestamp)"
) }}
