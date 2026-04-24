{% set chain = 'solana' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'extended_balances',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['day'],
    unique_key = ['day', 'address', 'token_mint_address'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.day')]
  )
}}

-- extended balances: tracks balances for newly added stablecoins (not in core list)
-- update start_date when adding new stablecoins

{{ stablecoins_svm_balances(
  blockchain = chain,
  token_list = 'extended',
  start_date = '2020-10-02'
) }}
