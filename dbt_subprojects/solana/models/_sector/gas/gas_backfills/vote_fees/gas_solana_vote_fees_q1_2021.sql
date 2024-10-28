{{ config(
    schema = 'gas_solana',
    alias = 'vote_fees_q1_2021',
    tags = ['static'],
    materialized = 'table',
    file_format = 'delta'
) }}

{{ solana_vote_fees_macro(
    "cast('2021-01-01' as timestamp)",
    "cast('2021-02-01' as timestamp)"
) }}
