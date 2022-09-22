{{config(alias='bridges')}}

SELECT * FROM {{ ref('labels_bridges_ethereum') }}
