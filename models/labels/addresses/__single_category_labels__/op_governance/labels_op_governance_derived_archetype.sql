{{config(
     alias = 'op_governance_derived_archetype'
)}}

{% set op_governance_labels_models = [
 ref('labels_op_governance_delegators')
 ,ref('labels_op_governance_retropgf_proposal_submitters')
 ,ref('labels_op_governance_retropgf_voters')
 ,ref('labels_op_governance_voters')
] %}


WITH joined_gov_labels AS
({% for model in op_governance_labels_models %}
SELECT *
FROM {{model}}
{% if not loop.last %}
UNION
{% endif %}
{% endfor %}
),

address_count AS
(SELECT address, COUNT(address) AS address_count
FROM joined_gov_labels
GROUP BY address
),

governance_junkie AS
(SELECT address, 'OP Governance Junkie' AS label
FROM address_count
WHERE address_count > 2
)

SELECT 'optimism' AS blockchain,
    address,
    label AS name,
    'op_governance' AS category,
    'kaiblade' AS contributor,
    'query' AS source,
    TIMESTAMP '2023-12-05' AS created_at,
    NOW() AS updated_at,
    'op_governance_derived_archetype' AS model_name,
    'persona' AS label_type
FROM
    governance_junkie
