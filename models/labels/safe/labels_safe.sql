{{config(alias='safe')}}

SELECT * FROM {{ ref('labels_safe_ethereum') }}