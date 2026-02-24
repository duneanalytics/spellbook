{% set chain = 'solana' %}

{{
  config(
    schema = 'stablecoins_' ~ chain,
    alias = 'transfers',
    materialized = 'view'
    , post_hook='{{ expose_spells(\'["solana"]\',
        "sector",
        "stablecoins",
        \'["tomfutago"]\') }}'
  )
}}

-- union of core and extended enriched transfers

select *
from {{ ref('stablecoins_' ~ chain ~ '_core_transfers_enriched') }}
union all
select *
from {{ ref('stablecoins_' ~ chain ~ '_extended_transfers_enriched') }}
