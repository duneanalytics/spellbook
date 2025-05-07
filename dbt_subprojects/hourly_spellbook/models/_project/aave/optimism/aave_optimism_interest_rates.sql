{{
  config(
    schema = 'aave_optimism',
    alias = 'interest_rates',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["optimism"]\',
                                  spell_type = "project",
                                  spell_name = "aave",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

{{
  lending_aave_compatible_interest_rates_view(
    blockchain = 'optimism'
  )
}}
