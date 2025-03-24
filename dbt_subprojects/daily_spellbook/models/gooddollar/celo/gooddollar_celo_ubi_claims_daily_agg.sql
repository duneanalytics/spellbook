{{
  config(
    schema = 'gooddollar_celo',
    alias = 'ubi_claims_daily_agg',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook = '{{ expose_spells(blockchains = \'["celo"]\',
                                  spell_type = "project",
                                  spell_name = "gooddollar",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with

ubi_daily as (
  select
    evt_block_time as block_time,
    date_trunc('day', evt_block_time) as block_date,
    evt_block_number as block_number,
    day as day_oridinal,
    dailyUbi / 1e18 as daily_ubi,
    dailyUbi as daily_ubi_raw,
    evt_tx_hash as tx_hash
  from {{ source('gooddollar_celo', 'ubischemev2_evt_ubicalculated') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('evt_block_time') }}
  {% endif %}
),

ubi_claimed as (
  select
    block_time,
    block_date,
    block_number,
    claimer,
    amount,
    amount_usd,
    tx_fee_usd,
    tx_hash
  from {{ ref('gooddollar_celo_ubi_claims') }}
  {% if is_incremental() %}
  where {{ incremental_predicate('block_time') }}
  {% endif %}
),

ubi_claimed_agg as (
  select
    block_date,
    count(*) as claim_count,
    count(distinct claimer) as unique_claimers,
    sum(amount) as amount,
    sum(amount_usd) as amount_usd,
    sum(tx_fee_usd) as tx_fee_usd
  from ubi_claimed
  group by 1
)

select
  ca.block_date,
  ca.unique_claimers,
  ca.claim_count,
  ca.amount,
  ca.amount / ca.claim_count as avg_amount,
  ca.amount_usd,
  ca.amount_usd / ca.claim_count as avg_amount_usd,
  d.day_oridinal,
  d.daily_ubi,
  d.daily_ubi_raw,
  ca.tx_fee_usd / nullif(ca.amount_usd, 0) as fee_to_claim_ratio
from ubi_claimed_agg ca
  left join ubi_daily d on ca.block_date = d.block_date
