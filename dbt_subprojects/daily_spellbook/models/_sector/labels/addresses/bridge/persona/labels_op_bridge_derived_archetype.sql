{{config(
     alias = 'op_bridge_derived_archetype'
)}}

WITH derived_bridge_archetype AS
(SELECT address, 'Prolific Bridge User' As label
FROM (SELECT address, COUNT(address) AS address_count
FROM
(SELECT *
FROM {{ ref('labels_op_bridge_users') }}
)
GROUP BY address
)
WHERE address_count >=3
)

SELECT 'optimism' AS blockchain,
    address,
    label AS name,
    'bridge' AS category,
    'kaiblade' AS contributor,
    'query' AS source,
    TIMESTAMP '2023-12-07' AS created_at,
    NOW() AS updated_at,
    'op_bridge_derived_archetype' AS model_name,
    'persona' AS label_type
FROM
    derived_bridge_archetype