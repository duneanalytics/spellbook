{{ config(
    alias = 'all_distributions_labels'
    )
}}

WITH all_labels AS (
    SELECT address, label, proposal_name, address_descriptor, project_name FROM {{ ref('op_token_distributions_optimism_project_wallets') }}
    UNION ALL
    SELECT address, label, NULL AS proposal_name, address_descriptor, address_descriptor AS project_name FROM {{ ref('op_token_distributions_optimism_disperse_contracts') }}
    UNION ALL
    SELECT address, 'CEX' as label, distinct_name AS proposal_name, cex_name AS proposal_name, cex_name AS project_name FROM {{ ref('addresses_optimism_cex') }}
        WHERE address NOT IN (SELECT address FROM {{ ref('op_token_distributions_optimism_project_wallets') }})
)

SELECT * FROM all_labels
