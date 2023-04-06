{{config(alias='institution',
        post_hook='{{ expose_spells(\'["ethereum","bnb","fantom", "optimism", "bitcoin"]\',
                                    "sector",
                                    "labels",
                                    \'["ilemi"]\') }}'
)}}

SELECT * FROM {{ ref('labels_cex') }}
UNION ALL
SELECT * FROM {{ ref('labels_funds') }}
