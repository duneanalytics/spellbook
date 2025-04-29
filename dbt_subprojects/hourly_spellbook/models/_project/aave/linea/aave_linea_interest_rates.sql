{{
  config(
    schema = 'aave_linea',
    alias = 'interest_rates',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["linea"]\',
                                  spell_type = "project",
                                  spell_name = "aave",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

{{
  lending_aave_compatible_interest_rates_view(
    blockchain = 'linea'
  )
}}
