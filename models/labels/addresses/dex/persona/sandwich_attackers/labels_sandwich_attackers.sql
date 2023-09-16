{{
    config(
        tags=['dunesql'],
        alias = alias('sandwich_attackers'), 
        post_hook='{{ expose_spells(\'["ethereum"]\', "sector", "labels", \'["alexth"]\') }}'
    )
}}

SELECT * FROM {{ ref('labels_sandwich_attackers_ethereum') }}