{{
  config(
    schema = 'aave_celo',
    alias = 'supply',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["celo"]\',
                                  spell_type = "project",
                                  spell_name = "aave",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

{{
  lending_aave_compatible_supply_view(
    blockchain = 'celo'
  )
}} 