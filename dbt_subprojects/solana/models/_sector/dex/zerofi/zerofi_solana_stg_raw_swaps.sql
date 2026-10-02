{{
  config(
    schema = 'zerofi_solana'
    , alias = 'stg_raw_swaps'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

{#
    ZeroFi swap instructions (first byte of instruction data):
      0x06 Swap    (original instruction, last called 2026-09-05)
      0x10 SwapV4  (live since 2026-05-12, now the only swap path)
    Both keep the market account first (pool_id = account_arguments[1]) and
    emit the same two inner token transfers in the same order (user -> vault, then
    vault -> user), so the transfer offsets in base_trades are shared.
#}
{{ solana_amm_stg_raw_swaps(
    program_id = 'ZERor4xhbUycZ6gb9ntrhqscUcZmAbQDjEAtCf4hbZY'
    , discriminator_filter = "BYTEARRAY_SUBSTRING(data, 1, 1) IN (0x06, 0x10)"
    , project_start_date = '2024-12-12'
    , pool_id_expression = "account_arguments[1]"
) }}
