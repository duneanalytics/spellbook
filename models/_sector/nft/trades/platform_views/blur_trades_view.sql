
{{ config(
        schema = 'blur',
        alias = alias('trades'),
        materialized = 'view',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "project",
                                    "blur",
                                    \'["0xRob"]\') }}')
}}

SELECT *
FROM {{ ref('nft_trades') }}
WHERE project = "blur"
