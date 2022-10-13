{{config(alias='validators',
        post_hook='{{ expose_spells(\'["ethereum","bnb"]\',
                                    "sector",
                                    "labels",
                                    \'["soispoke"]\') }}')}}

SELECT * FROM  {{ ref('labels_validators_ethereum') }}
UNION
SELECT * FROM  {{ ref('labels_validators_bnb') }}