{{
    config(
        alias='trader_dex_diversity',
        post_hook='{{ expose_spells(\'["ethereum"]\', "sector", "labels", \'["gentrexha"]\') }}'
    )
}}

SELECT * FROM {{ ref('trader_dex_diversity_ethereum') }}