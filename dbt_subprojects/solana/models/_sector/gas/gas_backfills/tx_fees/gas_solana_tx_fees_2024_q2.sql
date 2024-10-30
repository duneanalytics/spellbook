{{ config(
    schema = 'gas_solana',
    alias = 'tx_fees_2024_q2',
    tags = ['static'],
    partition_by = ['block_date', 'block_hour'],
    materialized = 'table',
    file_format = 'delta'
) }}

{{ solana_tx_fees_macro(
    "cast('2024-04-01' as timestamp)",
    "cast('2024-07-01' as timestamp)"
) }}
