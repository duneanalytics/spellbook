{{
  config(
    schema = 'aave_polygon',
    alias = 'supply',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["polygon"]\',
                                  spell_type = "project",
                                  spell_name = "aave",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

{{
  lending_aave_compatible_supply_view(
    blockchain = 'polygon'
  )
}} 