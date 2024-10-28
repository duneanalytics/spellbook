{{ config(
    schema = 'gas_solana',
    alias = 'vote_fees_2021_q3',
    tags = ['static'],
    materialized = 'table',
    file_format = 'delta'
) }}

{{ solana_vote_fees_macro(
    "cast('2021-07-01' as timestamp)",
    "cast('2021-10-01' as timestamp)"
) }}
