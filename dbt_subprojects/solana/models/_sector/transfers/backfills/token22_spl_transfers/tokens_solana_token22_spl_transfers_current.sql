{{ config(
    schema = 'tokens_solana',
    alias = 'token22_spl_transfers_current',
    partition_by = ['block_date'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'delete+insert',
    unique_key = ['block_date', 'tx_id', 'outer_instruction_index', 'inner_instruction_index', 'block_slot']
) }}

with results as (
{{ solana_token22_spl_transfers_macro(
    "cast('2024-10-01' as timestamp)",
    "now()"
) }}
)
select * from results
where block_date > now() - interval '7' day
