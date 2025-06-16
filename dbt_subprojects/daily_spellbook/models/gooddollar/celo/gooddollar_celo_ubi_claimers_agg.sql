{{
  config(
    schema = 'gooddollar_celo',
    alias = 'ubi_claimers_agg',
    materialized = 'view',
    unique_key = ['claimer'],
    post_hook = '{{ expose_spells(blockchains = \'["celo"]\',
                                  spell_type = "project",
                                  spell_name = "gooddollar",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with

ubi_claimed as (
  select
    block_time,
    block_date,
    block_number,
    claimer,
    amount,
    amount_usd,
    tx_fee_currency_symbol,
    tx_fee,
    tx_fee_usd,
    tx_hash
  from {{ ref('gooddollar_celo_ubi_claims') }}
),

claimer_agg as (
  select
    claimer,
    count(*) as claim_count,
    sum(amount) as amount,
    sum(amount_usd) as amount_usd,
    sum(tx_fee_usd) as tx_fee_usd
  from ubi_claimed
  group by 1
),

claimer_fee_details as (
  select
    claimer,
    tx_fee_currency_symbol,
    sum(tx_fee) as total_fee_by_currency
  from ubi_claimed
  group by 1, 2
),

claimer_fee_arrays as (
  select
    claimer,
    array_agg(tx_fee_currency_symbol) as fee_currency_symbols,
    array_agg(total_fee_by_currency) as fee_amounts_by_currency
  from claimer_fee_details
  group by 1
)

select
  ca.claimer,
  ca.claim_count,
  ca.amount,
  ca.amount_usd,
  ca.tx_fee_usd,
  cfa.fee_currency_symbols,
  cfa.fee_amounts_by_currency
from claimer_agg ca
  left join claimer_fee_arrays cfa on ca.claimer = cfa.claimer
