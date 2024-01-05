
{{config(
     alias = 'op_attestationstation_creators'
)}}

SELECT
    'optimism' AS blockchain,
    creator AS address,
    'Attestation Attester' AS name,
    'op_attestationstation' AS category,
    'kaiblade' AS contributor,
    'query' AS source,
    TIMESTAMP '2023-12-04' AS created_at,
    NOW() AS updated_at,
    'op_attestationstation_creators' AS model_name,
    'persona' AS label_type
FROM
    {{ source('attestationstation_optimism', 'AttestationStation_evt_AttestationCreated') }}

GROUP BY creator
