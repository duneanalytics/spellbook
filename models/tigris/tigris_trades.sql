{{ config(
        alias ='trades',
        post_hook='{{ expose_spells(\'["polygon","arbitrum"]\',
                                "project",
                                "tigris",
                                \'["henrystats"]\') }}'
        )
}}

SELECT * FROM  {{ ref('tigris_polygon_trades') }}

UNION 

SELECT * FROM {{ ref('tigris_arbitrum_trades') }}