{{ config(
    schema = 'gas_solana',
    alias = 'tx_fees_2022_q4',
    tags = ['static'],
    partition_by = ['block_date', 'block_hour'],
    materialized = 'table',
    file_format = 'delta'
) }}

{{ solana_tx_fees_macro(
    "cast('2022-10-01' as timestamp)",
    "cast('2023-01-01' as timestamp)"
) }}
