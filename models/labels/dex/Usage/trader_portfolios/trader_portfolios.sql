{{
    config(
        alias='trader_portfolios',
        post_hook='{{ expose_spells(\'["ethereum"]\', "sector", "labels", \'["gentrexha"]\') }}'
    )
}}

SELECT * FROM {{ ref('trader_portfolios_ethereum') }}