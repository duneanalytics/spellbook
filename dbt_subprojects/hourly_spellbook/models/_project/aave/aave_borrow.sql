{{
  config(
    schema = 'aave',
    alias = 'borrow',
    materialized = 'view',
    post_hook = '{{ expose_spells(blockchains = \'[
                                    "arbitrum",
                                    "avalanche_c",
                                    "base",
                                    "bnb",
                                    "celo",
                                    "ethereum",
                                    "fantom",
                                    "gnosis",
                                    "linea",
                                    "optimism",
                                    "polygon",
                                    "scroll",
                                    "sonic",
                                    "zksync"
                                  ]\',
                                  spell_type = "project",
                                  spell_name = "aave",
                                  contributors = \'["tomfutago"]\') }}'
  )
}}

{{
  lending_aave_compatible_borrow_view()
}}
