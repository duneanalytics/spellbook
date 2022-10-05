{{config(alias='arbitrage_traders')}}

SELECT * FROM {{ ref('labels_arbitrage_traders_ethereum') }}