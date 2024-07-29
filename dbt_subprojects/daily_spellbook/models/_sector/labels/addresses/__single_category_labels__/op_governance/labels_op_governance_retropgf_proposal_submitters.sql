{{config(
     alias = 'op_governance_retropgf_proposal_submitters'
)}}

WITH retropgf_proposal_submitters AS
(SELECT DISTINCT(recipient) as address, 'RetroPGF Proposal Submitter' AS label
-- FROM attestationstation_v1_optimism.EAS_evt_Attested
FROM {{ source('attestationstation_v1_optimism', 'EAS_evt_Attested') }}
WHERE schema = 0x76e98cce95f3ba992c2ee25cef25f756495147608a3da3aa2e5ca43109fe77cc
)

SELECT 'optimism' AS blockchain,
    address,
    label AS name,
    'op_governance' AS category,
    'kaiblade' AS contributor,
    'query' AS source,
    TIMESTAMP '2023-12-05' AS created_at,
    NOW() AS updated_at,
    'op_governance_retropgf_proposal_submitters' AS model_name,
    'persona' AS label_type
FROM
    retropgf_proposal_submitters
GROUP BY address, label


