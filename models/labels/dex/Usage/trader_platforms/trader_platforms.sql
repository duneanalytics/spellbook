{{
    config(
        alias='trader_platforms',
        post_hook='{{ expose_spells(\'["ethereum"]\', "sector", "labels", \'["gentrexha"]\') }}'
    )
}}

SELECT * FROM {{ ref('trader_platforms_ethereum') }}