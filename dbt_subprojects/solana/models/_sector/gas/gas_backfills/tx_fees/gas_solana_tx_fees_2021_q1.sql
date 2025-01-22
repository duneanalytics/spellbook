{{ config(
    schema = 'gas_solana',
    alias = 'tx_fees_2021_q1',
    tags = ['static'],
    partition_by = ['block_date', 'block_hour'],
    materialized = 'table',
    file_format = 'delta'
) }}

{{ solana_tx_fees_macro(
    "cast('2021-01-01' as timestamp)",
    "cast('2021-04-01' as timestamp)"
) }}
