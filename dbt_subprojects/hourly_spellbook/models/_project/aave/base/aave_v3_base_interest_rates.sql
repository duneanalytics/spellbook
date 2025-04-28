{{
  config(
    schema = 'aave_v3_base',
    alias = 'interest_rates',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["base"]\',
                                  spell_type = "project",
                                  spell_name = "aave",
                                  contributors = \'["mikeghen1","batwayne", "chuxin","tomfutago"]\') }}'
  )
}}

with aave_v3_base_interest_rates as (
  {{
    lending_aave_compatible_interest_rates_view(
      blockchain = 'base',
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
from aave_v3_base_interest_rates
