{{
  config(
    schema = 'aave_celo',
    alias = 'borrow',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["celo"]\',
                                  spell_type = "project",
                                  spell_name = "aave",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

{{
  lending_aave_compatible_borrow_view(
    blockchain = 'celo'
  )
}} 