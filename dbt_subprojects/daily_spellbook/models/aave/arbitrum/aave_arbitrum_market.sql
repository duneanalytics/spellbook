{{
  config(
    schema = 'aave_arbitrum',
    alias = 'market',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["arbitrum"]\',
                                  spell_type = "project",
                                  spell_name = "aave",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

{{
  lending_aave_compatible_market_view(
    blockchain = 'arbitrum'
  )
}}
