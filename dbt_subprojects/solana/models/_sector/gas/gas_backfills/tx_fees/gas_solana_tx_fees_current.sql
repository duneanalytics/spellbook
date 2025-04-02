{{ config(
    schema = 'gas_solana',
    alias = 'tx_fees_current',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')],
    unique_key = ['block_date', 'block_slot', 'tx_index']
) }}

{{ solana_tx_fees_macro(
    "cast('2025-01-01' as timestamp)",
    "now()"
) }}