{{ config(
    schema = 'gas_solana',
    alias = 'vote_fees_2022_q1',
    materialized = 'view'
) }}

{{ solana_vote_fees_macro(
    "cast('2022-01-01' as timestamp)",
    "cast('2022-04-01' as timestamp)"
) }}
