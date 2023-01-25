{{
    config(
        alias='trader_frequencies',
        post_hook='{{ expose_spells(\'["ethereum"]\', "sector", "labels", \'["gentrexha"]\') }}'
    )
}}

SELECT * FROM {{ ref('trader_frequencies_ethereum') }}