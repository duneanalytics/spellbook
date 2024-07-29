{{config(
        
        alias = 'funds',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["soispoke"]\') }}'
)}}

SELECT * FROM {{ ref('labels_funds_ethereum') }}
