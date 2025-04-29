{{
  config(
    schema = 'aave_gnosis',
    alias = 'flashloans',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["gnosis"]\',
                                  spell_type = "project",
                                  spell_name = "aave",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

{{
  lending_aave_compatible_flashloans_view(
    blockchain = 'gnosis'
  )
}} 