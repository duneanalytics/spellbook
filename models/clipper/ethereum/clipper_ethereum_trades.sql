{{ config(
        alias ='trades',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                "project",
                                "clipper",
                                \'["0xRob"]\') }}'
        )
}}

SELECT *
FROM (
    SELECT *
    FROM {{ ref('clipper_v1_ethereum_trades') }}
    UNION
    SELECT *
    FROM {{ ref('clipper_v2_ethereum_trades') }}
    UNION
    SELECT *
    FROM {{ ref('clipper_v3_ethereum_trades') }}
)
