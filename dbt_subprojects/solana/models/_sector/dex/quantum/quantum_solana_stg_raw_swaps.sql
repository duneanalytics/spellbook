{{
  config(
    schema = 'quantum_solana'
    , alias = 'stg_raw_swaps'
    , partition_by = ['block_month']
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
    , unique_key = ['block_month', 'block_date', 'surrogate_key']
  )
}}

{{ solana_amm_stg_raw_swaps(
    program_id = 'QuaNtZsgYRe5Z9Bk4LZ4cTD9tbkVoyCNf1R2BN9bBDv'
    , discriminator_filter = "BYTEARRAY_SUBSTRING(data, 1, 1) = 0x07 AND cardinality(account_arguments) >= 10"
    , project_start_date = '2026-01-28'
    , pool_id_expression = "account_arguments[4]"
) }}
