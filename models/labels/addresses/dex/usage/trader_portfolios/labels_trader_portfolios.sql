{{
    config(
        tags=[ 'prod_exclude'],
        alias = 'trader_portfolios',
        post_hook='{{ expose_spells(\'["ethereum"]\', "sector", "labels", \'["gentrexha"]\') }}'
    )
}}

SELECT * FROM {{ ref('labels_trader_portfolios_ethereum') }}