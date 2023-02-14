{{config(alias='bridges')}}

SELECT * FROM {{ ref('labels_bridges_ethereum') }}
UNION ALL
SELECT * FROM {{ ref('labels_bridges_fantom') }}
