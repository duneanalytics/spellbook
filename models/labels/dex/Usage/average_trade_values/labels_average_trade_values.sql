{{
    config(
        alias='average_trade_values',
        post_hook='{{ expose_spells(\'["ethereum"]\', "sector", "labels", \'["gentrexha"]\') }}'
    )
}}

SELECT * FROM {{ ref('labels_average_trade_values_ethereum') }}