{{ config(
    alias='trades',
    schema='firebird_finance_optimism',
    materialized='view'
    , post_hook='{{ hide_spells() }}'
    )
}}

SELECT *
FROM {{ ref('dex_aggregator_trades') }}
WHERE project = 'firebird_finance'
  AND blockchain = 'optimism'
