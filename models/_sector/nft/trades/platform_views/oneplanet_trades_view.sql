
{{ config(
        schema = 'oneplanet',
        alias = alias('trades'),
        materialized = 'view',
        post_hook='{{ expose_spells(\'["polygon"]\',
                                    "project",
                                    "oneplanet",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades') }}
WHERE project = "oneplanet"
