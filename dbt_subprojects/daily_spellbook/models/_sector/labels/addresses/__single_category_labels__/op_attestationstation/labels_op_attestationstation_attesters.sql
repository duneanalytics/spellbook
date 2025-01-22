{{config(
     alias = 'op_attestationstation_attesters'
)}}

SELECT
    'optimism' AS blockchain,
    attester AS address,
    'Attestation Attester' AS name,
    'op_attestationstation' AS category,
    'kaiblade' AS contributor,
    'query' AS source,
    TIMESTAMP '2023-12-04' AS created_at,
    NOW() AS updated_at,
    'op_attestationstation_attesters' AS model_name,
    'persona' AS label_type
FROM
    {{ source('attestationstation_v1_optimism', 'EAS_evt_Attested') }}

GROUP BY attester