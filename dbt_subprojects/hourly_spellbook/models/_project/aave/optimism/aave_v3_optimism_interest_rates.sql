{{
  config(
    schema = 'aave_v3_optimism',
    alias = 'interest_rates',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["optimism"]\',
                                  spell_type = "project",
                                  spell_name = "aave",
                                  contributors = \'["batwayne", "chuxin", "tomfutago"]\') }}'
  )
}}

with aave_v3_optimism_interest_rates as (
  {{
    lending_aave_compatible_interest_rates_view(
      blockchain = 'optimism',
      version = '3'
    )
  }}
)
select 
  blockchain,
  project,
  version,
  block_hour as hour,
  token_address as reserve,
  symbol,
  deposit_rate as deposit_apy,
  stable_borrow_rate as stable_borrow_apy,
  variable_borrow_rate as variable_borrow_apy
from aave_v3_optimism_interest_rates
