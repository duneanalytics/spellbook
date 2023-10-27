{{config(
     alias = 'arbitrage_traders'
    , post_hook='{{ expose_spells(\'["ethereum"]\', "sector", "labels", \'["alexth", "hosuke"]\') }}'
    )
}}

SELECT * FROM {{ ref('labels_arbitrage_traders_ethereum') }}