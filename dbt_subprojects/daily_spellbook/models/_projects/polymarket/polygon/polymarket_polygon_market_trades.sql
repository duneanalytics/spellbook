{{
  config(
    schema = 'polymarket_polygon',
    alias = 'market_trades',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_time', 'asset_id', 'evt_index', 'tx_hash'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
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

changed_tokens as (
  select distinct md.token_id
  from market_details md
  left join {{ this }} t
    on md.token_id = t.asset_id
  where t.asset_id is null
    or coalesce(cast(md.event_market_name as varchar), '') != coalesce(cast(t.event_market_name as varchar), '')
    or coalesce(cast(md.question as varchar), '') != coalesce(cast(t.question as varchar), '')
    or coalesce(cast(md.polymarket_link as varchar), '') != coalesce(cast(t.polymarket_link as varchar), '')
    or coalesce(cast(md.token_outcome as varchar), '') != coalesce(cast(t.token_outcome as varchar), '')
    or coalesce(cast(md.neg_risk as varchar), '') != coalesce(cast(t.neg_risk as varchar), '')
    or coalesce(cast(md.unique_key as varchar), '') != coalesce(cast(t.unique_key as varchar), '')
    or coalesce(cast(md.token_outcome_name as varchar), '') != coalesce(cast(t.token_outcome_name as varchar), '')
),

source_trades as (
  select
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
    taker
  from (
    select
      t.*,
      row_number() over (
        partition by t.block_time, t.asset_id, t.evt_index, t.tx_hash
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
    taker
  from {{ ref('polymarket_polygon_market_trades_raw') }}
)

{% endif -%}

select
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
  md.unique_key,
  md.token_outcome_name
from source_trades t
left join market_details md
  on t.asset_id = md.token_id
