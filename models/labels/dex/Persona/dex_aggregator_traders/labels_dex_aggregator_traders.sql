{{
    config(
        alias='dex_aggregator_traders',
        post_hook='{{ expose_spells(\'["ethereum"]\', "sector", "labels", \'["gentrexha"]\') }}'
    )
}}

SELECT * FROM {{ ref('labels_dex_aggregator_traders_ethereum') }}