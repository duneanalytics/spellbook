{{config(alias='institution',
        post_hook='{{ expose_spells(\'["ethereum", "bnb", "fantom", "optimism", "bitcoin", "polygon", "avalanche_c", "arbitrum"]\',
                                    "sector",
                                    "labels",
                                    \'["ilemi", "hildobby"]\') }}'
)}}

SELECT * FROM {{ ref('labels_cex') }}
UNION ALL
SELECT * FROM {{ ref('labels_funds') }}
