{{
  config(
    schema = 'aave_arbitrum',
    alias = 'flashloans',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["arbitrum"]\',
                                  spell_type = "project",
                                  spell_name = "aave",
                                  contributors = \'["hildobby", "tomfutago"]\') }}'
  )
}}

{{
  lending_aave_compatible_flashloans_view(
    blockchain = 'arbitrum'
  )
}}
