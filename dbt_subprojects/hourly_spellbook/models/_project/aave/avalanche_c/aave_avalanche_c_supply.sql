{{
  config(
    schema = 'aave_avalanche_c',
    alias = 'supply',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["avalanche_c"]\',
                                  spell_type = "project",
                                  spell_name = "aave",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

{{
  lending_aave_compatible_supply_view(
    blockchain = 'avalanche_c'
  )
}} 