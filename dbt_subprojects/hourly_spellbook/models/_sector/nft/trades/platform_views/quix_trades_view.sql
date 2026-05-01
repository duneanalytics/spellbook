
{{ config(
        schema = 'quix',
        alias = 'trades',
        
        materialized = 'view'
        , post_hook='{{ hide_spells() }}'
)
}}

SELECT *
FROM {{ ref('nft_trades') }}
WHERE project = 'quix'
