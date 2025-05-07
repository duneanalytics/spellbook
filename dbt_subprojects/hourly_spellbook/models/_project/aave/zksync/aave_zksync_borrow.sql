{{
  config(
    schema = 'aave_zksync',
    alias = 'borrow',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'["zksync"]\',
                                  spell_type = "project",
                                  spell_name = "aave",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

{{
  lending_aave_compatible_borrow_view(
    blockchain = 'zksync'
  )
}} 