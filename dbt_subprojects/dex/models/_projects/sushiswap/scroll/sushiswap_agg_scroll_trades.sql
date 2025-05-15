{% set chain = 'scroll' %}

{{ config(
  schema = 'sushiswap_' ~ chain,
  alias  = 'trades',
  materialized = 'incremental',
  partition_by = ['block_month'],
  file_format = 'delta',
  incremental_strategy = 'merge',
  unique_key = ['blockchain','tx_hash','trace_address'],
  tags = [chain,'sushiswap','trades','dex','aggregator'],
  incremental_predicates = [
    incremental_predicate('DBT_INTERNAL_DEST.block_date')
  ]
) }}

{{ generate_sushiswap_trades(chain) }}