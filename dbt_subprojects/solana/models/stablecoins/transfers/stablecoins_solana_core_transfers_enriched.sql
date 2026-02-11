{% set chain = 'solana' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'core_transfers_enriched',
    materialized = 'view'
  )
}}

-- core transfers enriched with metadata and FX rate pricing

{{ stablecoins_svm_transfers_enrich(
  base_transfers = ref('stablecoins_' ~ chain ~ '_core_transfers'),
  blockchain = chain
) }}
