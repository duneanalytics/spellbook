{% set chain = 'arbitrum' %}

{{ config(
  schema = 'sushiswap_' ~ chain,
  alias  = 'trades',
  materialized = 'incremental',
  partition_by = ['block_month'],
  file_format = 'delta',
  incremental_strategy = 'merge',
  unique_key = ['block_date','blockchain','project','version','tx_hash','evt_index','trace_address'],
  tags = [chain,'sushiswap','trades','dex','aggregator'],
  incremental_predicates = [
    incremental_predicate('DBT_INTERNAL_DEST.block_date')
  ]
) }}

{{ generate_sushiswap_trades(chain) }}