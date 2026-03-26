{{
  config(
    schema = 'dex_tron',
    alias = 'trades',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['blockchain', 'project', 'version', 'tx_hash', 'evt_index', 'block_month'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    merge_skip_unchanged = true
  )
}}

with dexs as (
  {{
    enrich_dex_trades(
      base_trades = ref('dex_tron_base_trades'),
      filter = "1=1",
      tokens_erc20_model = source('tokens', 'erc20'),
      blockchain = 'tron'
    )
  }}
)

select
  blockchain,
  project,
  version,
  block_month,
  block_date,
  block_time,
  block_number,
  token_bought_symbol,
  token_sold_symbol,
  token_pair,
  token_bought_amount,
  token_sold_amount,
  token_bought_amount_raw,
  token_sold_amount_raw,
  amount_usd,
  to_tron_address(token_bought_address) as token_bought_address,
  to_tron_address(token_sold_address) as token_sold_address,
  to_tron_address(taker) as taker,
  to_tron_address(maker) as maker,
  to_tron_address(project_contract_address) as project_contract_address,
  lower(to_hex(tx_hash)) as tx_hash,
  to_tron_address(tx_from) as tx_from,
  to_tron_address(tx_to) as tx_to,
  evt_index
  , _updated_at
from dexs
