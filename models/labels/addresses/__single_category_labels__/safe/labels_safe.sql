{{config(
        tags=['dunesql']
        ,alias = alias('safe'),
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["soispoke"]\') }}'
)}}

SELECT * FROM {{ ref('labels_safe_ethereum') }}