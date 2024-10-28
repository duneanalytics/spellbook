{{ config(
    schema = 'gas_solana',
    alias = 'vote_fees_current',
    partition_by = ['block_date', 'block_hour'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_date', 'block_hour', 'block_slot', 'tx_index']
) }}

{{ solana_vote_fees_macro(
    "cast('2024-10-01' as timestamp)",
    "cast('current_date' as timestamp)"
) }}