
{{ config(
        schema = 'x2y2',
        alias = 'trades',
        
        materialized = 'view'
        , post_hook='{{ hide_spells() }}'
)
}}

SELECT *
FROM {{ ref('nft_trades') }}
WHERE project = 'x2y2'
