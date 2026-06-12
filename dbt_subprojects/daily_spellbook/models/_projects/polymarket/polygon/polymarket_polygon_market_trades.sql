{{
  config(
    schema = 'polymarket_polygon',
    alias = 'market_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['block_month'],
    unique_key = ['block_month', 'block_time', 'asset_id', 'evt_index', 'tx_hash', 'contract_address'],
    merge_skip_unchanged = true,
    post_hook = '{{ expose_spells(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "polymarket",
                                  contributors = \'["tomfutago, 0xboxer"]\') }}'
  )
}}

with market_details as (
  select
    token_id,
    event_market_name,
    question,
    polymarket_link,
    token_outcome,
    neg_risk,
    unique_key,
    token_outcome_name
  from {{ ref('polymarket_polygon_market_details') }}
),

{% if is_incremental() -%}

-- tokens whose latest already-merged row disagrees with market_details on any
-- metadata column. Tokens with no rows in {{ this }} have nothing to update;
-- their trades arrive through the recent-window branch below.
changed_tokens as (
  select md.token_id
  from market_details md
  inner join (
    select
      asset_id,
      max_by(event_market_name, block_time) as event_market_name,
      max_by(question, block_time) as question,
      max_by(polymarket_link, block_time) as polymarket_link,
      max_by(token_outcome, block_time) as token_outcome,
      max_by(neg_risk, block_time) as neg_risk,
      max_by(unique_key, block_time) as unique_key,
      max_by(token_outcome_name, block_time) as token_outcome_name
    from {{ this }}
    group by asset_id
  ) t on md.token_id = t.asset_id
  where coalesce(cast(md.event_market_name as varchar), '') != coalesce(cast(t.event_market_name as varchar), '')
    or coalesce(cast(md.question as varchar), '') != coalesce(cast(t.question as varchar), '')
    or coalesce(cast(md.polymarket_link as varchar), '') != coalesce(cast(t.polymarket_link as varchar), '')
    or coalesce(cast(md.token_outcome as varchar), '') != coalesce(cast(t.token_outcome as varchar), '')
    or coalesce(cast(md.neg_risk as varchar), '') != coalesce(cast(t.neg_risk as varchar), '')
    or coalesce(cast(md.unique_key as varchar), '') != coalesce(cast(t.unique_key as varchar), '')
    or coalesce(cast(md.token_outcome_name as varchar), '') != coalesce(cast(t.token_outcome_name as varchar), '')
),

source_trades as (
  select
    block_month,
    block_number,
    block_time,
    tx_hash,
    evt_index,
    action,
    contract_address,
    condition_id,
    asset_id,
    price,
    amount,
    shares,
    fee,
    maker,
    taker,
    is_taker_side,
    maker_side,
    taker_side,
    contract_version,
    builder,
    metadata
  from (
    select
      t.*,
      row_number() over (
        partition by t.block_month, t.block_time, t.asset_id, t.evt_index, t.tx_hash, t.contract_address
        order by t.block_time desc
      ) as rn
    from (
      select t.*
      from {{ ref('polymarket_polygon_market_trades_raw') }} t
      where {{ incremental_predicate('t.block_time') }}
      union all
      select t.*
      from {{ ref('polymarket_polygon_market_trades_raw') }} t
      inner join changed_tokens ct on t.asset_id = ct.token_id
    ) t
  ) deduped
  where rn = 1
)

{% else -%}

source_trades as (
  select
    block_month,
    block_number,
    block_time,
    tx_hash,
    evt_index,
    action,
    contract_address,
    condition_id,
    asset_id,
    price,
    amount,
    shares,
    fee,
    maker,
    taker,
    is_taker_side,
    maker_side,
    taker_side,
    contract_version,
    builder,
    metadata
  from {{ ref('polymarket_polygon_market_trades_raw') }}
)

{% endif -%}

select
  t.block_month,
  t.block_number,
  t.block_time,
  t.tx_hash,
  t.evt_index,
  t.action,
  t.contract_address,
  t.condition_id,
  md.event_market_name,
  md.question,
  md.polymarket_link,
  md.token_outcome,
  md.neg_risk,
  t.asset_id,
  t.price,
  t.amount,
  t.shares,
  t.fee,
  t.maker,
  t.taker,
  t.is_taker_side,
  t.maker_side,
  t.taker_side,
  t.contract_version,
  t.builder,
  t.metadata,
  md.unique_key,
  md.token_outcome_name,
  now() as _updated_at
from source_trades t
left join market_details md
  on t.asset_id = md.token_id
