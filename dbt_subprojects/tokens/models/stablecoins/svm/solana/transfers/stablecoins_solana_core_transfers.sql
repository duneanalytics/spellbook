{%- set chain = 'solana' -%}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'core_transfers',
    materialized = 'incremental',
    incremental_strategy = 'microbatch',
    event_time = 'block_date',
    begin = '2026-01-01',
    batch_size = 'day',
    partition_by = ['block_month'],
    unique_key = ['block_month', 'block_date', 'unique_key'],
    lookback = 1
  )
}}

-- core transfers: tracks transfers for stablecoins in the frozen core list

{{ stablecoins_svm_transfers(
    blockchain = chain,
    token_list = 'core'
) }}
