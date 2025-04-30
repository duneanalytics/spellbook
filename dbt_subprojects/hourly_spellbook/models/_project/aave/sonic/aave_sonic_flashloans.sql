{{
  config(
    schema = 'aave_sonic',
    alias = 'flashloans',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["sonic"]\',
                                  spell_type = "project",
                                  spell_name = "aave",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

{{
  lending_aave_compatible_flashloans_view(
    blockchain = 'sonic'
  )
}} 