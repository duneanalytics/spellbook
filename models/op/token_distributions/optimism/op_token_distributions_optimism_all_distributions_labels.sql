{{ config(
    alias = 'all_distributions_labels',
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "project",
                                "op_token_distributions",
                                \'["msilb7"]\') }}'
    )
}}

WITH all_labels AS (
    SELECT address
         , label
         , proposal_name
         , address_descriptor
         , project_name
    FROM {{ ref('op_token_distributions_optimism_project_wallets') }}

    UNION ALL

    SELECT
        contract_address as address
        , 'Utility' as label
        , 'Utility Contract' AS proposal_name
        , contract_name AS address_descriptor
        , contract_name AS project_name
    FROM {{ ref('contracts_optimism_disperse_contracts') }}
    WHERE contract_address NOT IN (SELECT address FROM {{ ref('op_token_distributions_optimism_project_wallets') }})

    UNION ALL

    SELECT
        address
        , 'CEX' as label
        , distinct_name AS proposal_name
        , cex_name AS address_descriptor
        , cex_name AS project_name
    FROM {{ ref('cex_optimism_addresses') }}
    WHERE address NOT IN (SELECT address FROM {{ ref('op_token_distributions_optimism_project_wallets') }})
)

SELECT * FROM all_labels
