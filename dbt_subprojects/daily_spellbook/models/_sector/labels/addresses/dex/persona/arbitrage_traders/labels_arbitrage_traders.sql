{{config(
     alias = 'arbitrage_traders'
    , post_hook='{{ hide_spells() }}'
    )
}}

SELECT * FROM {{ ref('labels_arbitrage_traders_ethereum') }}