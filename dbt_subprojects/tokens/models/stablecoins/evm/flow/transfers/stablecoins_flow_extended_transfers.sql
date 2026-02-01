{% set chain = 'flow' %}

{{
  config(
    tags = ['prod_exclude'],
    schema = 'stablecoins_' ~ chain,
    alias = 'extended_transfers',
    materialized = 'incremental',
    incremental_strategy = 'merge',
    partition_by = ['block_month'],
    unique_key = ['block_month', 'block_date', 'unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
  )
}}

-- extended transfers: tracks transfers for newly added stablecoins (not in core list)

{{ stablecoins_transfers(
    blockchain = chain,
    token_list = 'extended'
) }}
