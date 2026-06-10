{{ config(
    schema = 'openocean',
    alias = 'trades',
    materialized = 'view'
    , post_hook='{{ hide_spells() }}'
    )
}}

SELECT *
FROM {{ ref('dex_aggregator_trades') }}
WHERE project = 'openocean'
