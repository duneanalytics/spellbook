{{ config(
    schema = 'gas_solana',
    alias = 'vote_fees_2022_q3',
    materialized = 'view'
) }}

{{ solana_vote_fees_macro(
    "cast('2022-07-01' as timestamp)",
    "cast('2022-10-01' as timestamp)"
) }}
