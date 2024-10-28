{{ config(
    schema = 'gas_solana',
    alias = 'tx_fees_2022_q1',
    tags = ['static'],
    materialized = 'table',
    file_format = 'delta'
) }}

{{ solana_tx_fees_macro(
    "cast('2022-01-01' as timestamp)",
    "cast('2022-04-01' as timestamp)"
) }}
