{% set chain = 'peaq' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'core_transfers',
    materialized = 'incremental',
    incremental_strategy = 'merge',
    partition_by = ['block_month'],
    unique_key = ['block_month', 'block_date', 'unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
  )
}}

-- core transfers: tracks transfers for stablecoins in the frozen core list

{{ stablecoins_transfers(
    blockchain = chain,
    token_list = 'core'
) }}
