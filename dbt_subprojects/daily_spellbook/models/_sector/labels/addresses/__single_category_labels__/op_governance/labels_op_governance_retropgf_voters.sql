{{config(
     alias = 'op_governance_retropgf_voters'
)}}

WITH retropgf_voter AS
(SELECT DISTINCT(recipient) as address, 'RetroPGF Voter' AS label
-- FROM attestationstation_v1_optimism.EAS_evt_Attested
FROM {{ source('attestationstation_v1_optimism', 'EAS_evt_Attested') }}
WHERE schema = 0x3743be2afa818ee40304516c153427be55931f238d961af5d98653a93192cdb3
AND attester = 0x621477dba416e12df7ff0d48e14c4d20dc85d7d9
AND uid NOT IN 
(SELECT uid 
-- FROM attestationstation_v1_optimism.EAS_evt_Revoked
FROM {{ source('attestationstation_v1_optimism', 'EAS_evt_Revoked') }}
WHERE schema = 0x3743be2afa818ee40304516c153427be55931f238d961af5d98653a93192cdb3
AND attester = 0x621477dba416e12df7ff0d48e14c4d20dc85d7d9)
)

SELECT 'optimism' AS blockchain,
    address,
    label AS name,
    'op_governance' AS category,
    'kaiblade' AS contributor,
    'query' AS source,
    TIMESTAMP '2023-12-05' AS created_at,
    NOW() AS updated_at,
    'op_governance_retropgf_voters' AS model_name,
    'persona' AS label_type
FROM
    retropgf_voter
GROUP BY address, label


