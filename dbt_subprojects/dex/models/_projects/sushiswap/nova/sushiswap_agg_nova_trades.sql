{% set chain = 'nova' %}

{{ config(
  schema = 'sushiswap_agg_' + chain,
  alias = 'aggregator_trades',
  materialized = 'table',
  partition_by = ['block_month'],
  file_format = 'delta',
  tags = [chain,'sushiswap','trades','dex','aggregator','static'],
) }}

{{ generate_sushiswap_trades(chain) }}