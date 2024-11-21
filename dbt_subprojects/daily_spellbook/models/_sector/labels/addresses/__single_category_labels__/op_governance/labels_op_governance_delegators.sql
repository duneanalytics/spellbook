{{config(
     alias = 'op_governance_delegators'
)}}

WITH op_delegators AS
(SELECT DISTINCT(delegator) AS address, 'OP Delegator' AS label
-- FROM op_optimism.GovernanceToken_evt_DelegateChanged
FROM {{ source('op_optimism', 'GovernanceToken_evt_DelegateChanged') }}
WHERE toDelegate != 0x0000000000000000000000000000000000000000
)

SELECT 'optimism' AS blockchain,
    address,
    label AS name,
    'op_governance' AS category,
    'kaiblade' AS contributor,
    'query' AS source,
    TIMESTAMP '2023-12-05' AS created_at,
    NOW() AS updated_at,
    'op_governance_delegators' AS model_name,
    'persona' AS label_type
FROM
    op_delegators
GROUP BY address, label