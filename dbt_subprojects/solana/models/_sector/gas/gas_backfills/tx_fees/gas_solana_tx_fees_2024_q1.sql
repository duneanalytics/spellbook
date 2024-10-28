{{ config(
    schema = 'gas_solana',
    alias = 'tx_fees_2024_q1',
    tags = ['static'],
    materialized = 'table',
    file_format = 'delta'
) }}

{{ solana_tx_fees_macro(
    "cast('2024-01-01' as timestamp)",
    "cast('2024-04-01' as timestamp)"
) }}
