{{
  config(
    schema = 'gooddollar_celo',
    alias = 'reserve_movement',
    materialized = 'view',
    unique_key = ['block_date'],
    post_hook = '{{ expose_spells(blockchains = \'["celo"]\',
                                  spell_type = "project",
                                  spell_name = "gooddollar",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

with

reserve_base as (
  select
    block_date,
    token_in_symbol,
    token_out_symbol,
    token_in_amount,
    token_out_amount
  from {{ ref('gooddollar_celo_reserve') }}
),

reserve_daily_movement as (
  select
    block_date,
    sum(case when token_in_symbol = 'cUSD' then token_in_amount else 0 end) as cusd_in,
    -1 * sum(case when token_out_symbol = 'cUSD' then token_out_amount else 0 end) as cusd_out,
    sum(case when token_in_symbol = 'G$' then token_in_amount else 0 end) as gd_in,
    -1 * sum(case when token_out_symbol = 'G$' then token_out_amount else 0 end) as gd_out
  from reserve_base
  group by 1
)

select
  block_date,
  cusd_in,
  cusd_out,
  gd_in,
  gd_out,
  cusd_in + cusd_out as cusd_net,
  gd_in + gd_out as gd_net,
  sum(cusd_in + cusd_out) over (order by block_date) as cusd_balance,
  sum(gd_in + gd_out) over (order by block_date) as gd_balance
from reserve_daily_movement
