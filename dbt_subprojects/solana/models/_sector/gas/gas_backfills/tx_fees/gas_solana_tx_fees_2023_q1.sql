{{ config(
    schema = 'gas_solana',
    alias = 'tx_fees_2023_q1',
    tags = ['static'],
    materialized = 'table',
    file_format = 'delta'
) }}

{{ solana_tx_fees_macro(
    "cast('2023-01-01' as timestamp)",
    "cast('2023-04-01' as timestamp)"
) }}
