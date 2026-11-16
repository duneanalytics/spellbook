{% set chain = 'blast' %}

{{ config(
  schema = 'sushiswap_agg_' + chain,
  alias = 'aggregator_trades',
  materialized = 'table',
  partition_by = ['block_month'],
  file_format = 'delta',
  tags = [chain,'sushiswap','trades','dex','aggregator','static'],
  post_hook='{{ hide_spells() }}'
) }}

{{ generate_sushiswap_trades(chain) }}