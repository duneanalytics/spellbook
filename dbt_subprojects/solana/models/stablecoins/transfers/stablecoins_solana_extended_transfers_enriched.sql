{% set chain = 'solana' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'extended_transfers_enriched',
    materialized = 'view'
  )
}}

-- extended transfers enriched with metadata and FX rate pricing

{{ stablecoins_svm_transfers_enrich(
  base_transfers = ref('stablecoins_' ~ chain ~ '_extended_transfers'),
  blockchain = chain
) }}
