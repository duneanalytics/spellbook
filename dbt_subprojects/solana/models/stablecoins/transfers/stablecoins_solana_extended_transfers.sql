{%- set chain = 'solana' -%}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'extended_transfers',
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

-- extended transfers: tracks transfers for newly added stablecoins (not in core list)
-- update begin date when adding new stablecoins

{{ stablecoins_svm_transfers(
    blockchain = chain,
    token_list = 'extended'
) }}
