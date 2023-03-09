{{
    config(
        alias='contract_deployers_ethereum',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby", "hosuke"]\') }}'
    )
}}

WITH creation AS (
    SELECT distinct 'ethereum'           AS blockchain
                  , creation.`from`      AS address
                  , 'Contract Deployer'  as name
                  , 'infrastructure'     as category
                  , 'hildobby'           as contributor
                  , 'query'              AS source
                  , date('2023-03-03')   AS created_at
                  , NOW()                as modified_at
                  , 'contract_deployers' AS model_name
                  , 'persona'            as label_type
    FROM {{ source('ethereum', 'creation_traces') }}
)
SELECT *
FROM creation
LEFT ANTI JOIN {{ source('ethereum', 'creation_traces') }} anti_table
ON creation.address = anti_table.address