{% set chain = 'zkevm' %}

{{ config(
  schema = 'sushiswap_' ~ chain,
  alias  = 'sushiswap_agg_' ~ chain ~ '_trades',
  materialized = 'incremental',
  partition_by = ['block_month'],
  file_format = 'delta',
  incremental_strategy = 'merge',
  unique_key           = [
    'block_date','blockchain','project','version',
    'tx_hash','evt_index','trace_address'
  ],
  tags = [chain,'sushiswap','trades','dex','aggregator'],
  incremental_predicates = [ incremental_predicate('call_block_time') ]
) }}

{{ generate_sushiswap_trades(chain) }}