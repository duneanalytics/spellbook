{{config(
    
    alias = 'smart_dex_traders'
    , post_hook='{{ hide_spells() }}'
)}}

SELECT * FROM {{ ref('labels_smart_dex_traders_ethereum') }}