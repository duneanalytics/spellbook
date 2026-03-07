{{ config(
    alias = 'trades',
    schema = 'unidex_optimism',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'
    )
}}


SELECT *
FROM {{ ref('dex_aggregator_trades') }}
WHERE project = 'unidex'
  AND blockchain = 'optimism'