{{
  config(
    schema = 'aave',
    alias = 'interest_rates',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["avalanche_c", "arbitrum", "base", "bnb", "celo", "ethereum", "fantom", "gnosis", "linea", "optimism", "polygon", "scroll", "sonic", "zksync"]\',
                                  spell_type = "project",
                                  spell_name = "aave",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

{{
  lending_aave_compatible_interest_rates_view()
}}
