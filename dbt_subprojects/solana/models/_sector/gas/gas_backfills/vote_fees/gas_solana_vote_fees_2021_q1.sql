{{ config(
    schema = 'gas_solana',
    alias = 'vote_fees_2021_q1',
    materialized = 'view'
) }}

{{ solana_vote_fees_macro(
    "cast('2021-01-01' as timestamp)",
    "cast('2021-04-01' as timestamp)"
) }}
