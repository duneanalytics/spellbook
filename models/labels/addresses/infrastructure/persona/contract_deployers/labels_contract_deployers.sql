{{
    config(
        alias='contract_deployers',
        post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "bnb", "ethereum", "fantom", "gnosis","goerli","optimism","polygon"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby", "hosuke"]\') }}'
    )
}}

SELECT * FROM  {{ ref('labels_contract_deployers_arbitrum') }}
UNION
SELECT * FROM  {{ ref('labels_contract_deployers_avalanche_c') }}
UNION
SELECT * FROM  {{ ref('labels_contract_deployers_bnb') }}
UNION
SELECT * FROM  {{ ref('labels_contract_deployers_ethereum') }}
UNION
SELECT * FROM  {{ ref('labels_contract_deployers_fantom') }}
UNION
SELECT * FROM  {{ ref('labels_contract_deployers_gnosis') }}
UNION
SELECT * FROM  {{ ref('labels_contract_deployers_goerli') }}
UNION
SELECT * FROM  {{ ref('labels_contract_deployers_optimism') }}
UNION
SELECT * FROM  {{ ref('labels_contract_deployers_polygon') }}