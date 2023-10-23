{{config(
    
    alias = 'l2_fee_vaults',
    post_hook='{{ expose_spells(\'["optimism"]\',
                                "sector",
                                "labels",
                                \'["msilb7"]\') }}'
)}}

SELECT 'optimism'        AS blockchain
     , address
     , vault_name        AS name
     , 'infrastructure'  AS category
     , 'msilb7'          AS contributor
     , 'static'          AS source
     , TIMESTAMP '2023-06-02' AS created_at
     , now()             AS updated_at
     , 'l2_fee_vaults'   AS model_name
     , 'identifier'      AS label_type

FROM {{ ref('addresses_optimism_fee_vaults') }}
