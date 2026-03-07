{% set chain = 'solana' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'core_transfers_enriched',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    partition_by = ['block_month'],
    unique_key = ['block_month', 'block_date', 'unique_key'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_date')]
  )
}}

-- core transfers enriched with metadata and FX rate pricing

{{ stablecoins_svm_transfers_enrich(
  base_transfers = ref('stablecoins_' ~ chain ~ '_core_transfers'),
  blockchain = chain
) }}
