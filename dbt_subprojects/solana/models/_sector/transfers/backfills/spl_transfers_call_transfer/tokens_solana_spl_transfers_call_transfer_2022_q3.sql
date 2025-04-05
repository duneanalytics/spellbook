{{ config(
    schema = 'tokens_solana',
    alias = 'spl_transfers_call_transfer_2022_q3',
    tags = ['static'],
    partition_by = ['block_date'],
    materialized = 'table',
    file_format = 'delta'
) }}

{{ solana_spl_transfers_call_transfer_macro(
    "cast('2022-07-01' as timestamp)",
    "cast('2022-10-01' as timestamp)"
) }} 