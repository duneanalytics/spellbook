{{config(alias='token_standards',
        post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "bnb", "ethereum", "fantom", "gnosis","goerli","optimism","polygon"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby"]\') }}')}}

SELECT * FROM  {{ ref('labels_token_standards_arbitrum') }}
UNION
SELECT * FROM  {{ ref('labels_token_standards_avalanche_c') }}
UNION
SELECT * FROM  {{ ref('labels_token_standards_bnb') }}
UNION
SELECT * FROM  {{ ref('labels_token_standards_ethereum') }}
UNION
SELECT * FROM  {{ ref('labels_token_standards_ethereum') }}
UNION
SELECT * FROM  {{ ref('labels_token_standards_fantom') }}
UNION
SELECT * FROM  {{ ref('labels_token_standards_gnosis') }}
UNION
SELECT * FROM  {{ ref('labels_token_standards_goerli') }}
UNION
SELECT * FROM  {{ ref('labels_token_standards_optimism') }}
UNION
SELECT * FROM  {{ ref('labels_token_standards_polygon') }}