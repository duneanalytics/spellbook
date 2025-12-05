{{
   config(
     schema = 'stablecoins',
     alias = 'balances',
     materialized = 'incremental',
     file_format = 'delta',
     incremental_strategy = 'merge',
     partition_by = ['day'],
     unique_key = ['day', 'blockchain', 'address', 'token_address'],
     incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')],
     post_hook = '{{ expose_spells(blockchains = \'["arbitrum","avalanche_c","base","bnb","ethereum","kaia","linea","optimism","polygon","scroll","worldchain","zksync"]\',
                                  spell_type = "sector",
                                  spell_name = "stablecoins",
                                  contributors = \'["tomfutago"]\') }}'
   )
 }}

{{
  balances_incremental_subset_daily_enrich(
    base_balances = ref('stablecoins_base_balances')
  )
}}
