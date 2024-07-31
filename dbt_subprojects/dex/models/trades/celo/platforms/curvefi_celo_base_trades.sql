{{
    config(
        schema = 'curvefi_celo',
        alias = 'base_trades',
        materialized = 'incremental',
        file_format = 'delta',
        incremental_strategy = 'merge',
        unique_key = ['tx_hash', 'evt_index'],
        incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
    )
}}

with base_trades as (
  -- Stableswap
  select
    'stable' as pool_type,
    t.evt_block_time as block_time,
    t.evt_block_number as block_number,
    t.buyer as taker,
    cast(null as varbinary) as maker,
    t.tokens_bought as token_bought_amount_raw,
    t.tokens_sold as token_sold_amount_raw,
    pool_bought.token as token_bought_address,
    pool_sold.token as token_sold_address,
    t.contract_address as project_contract_address,
    t.evt_tx_hash as tx_hash,
    t.evt_index,
    t.bought_id,
    t.sold_id
  from {{ source('curvefi_celo', 'StableSwap_evt_TokenExchange') }} t
    join {{ ref('curvefi_celo_pools') }} pool_bought on t.contract_address = pool_bought.pool and t.bought_id = pool_bought.tokenid
    join {{ ref('curvefi_celo_pools') }} pool_sold on t.contract_address = pool_sold.pool and t.sold_id = pool_sold.tokenid
  {% if is_incremental() %}
  where {{ incremental_predicate('t.evt_block_time') }}
  {% endif %}
)

select
  'celo' as blockchain,
  'curve' as project,
  '1' as version,
  cast(date_trunc('month', block_time) as date) as block_month,
  cast(block_time as date) as block_date,
  block_time,
  block_number,
  token_bought_amount_raw,
  token_sold_amount_raw,
  token_bought_address,
  token_sold_address,
  taker,
  maker,
  project_contract_address,
  tx_hash,
  evt_index
from base_trades
