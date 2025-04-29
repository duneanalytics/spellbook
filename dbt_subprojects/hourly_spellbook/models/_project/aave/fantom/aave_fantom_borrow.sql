{{
  config(
    schema = 'aave_fantom',
    alias = 'borrow',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["fantom"]\',
                                  spell_type = "project",
                                  spell_name = "aave",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

{{
  lending_aave_compatible_borrow_view(
    blockchain = 'fantom'
  )
}} 