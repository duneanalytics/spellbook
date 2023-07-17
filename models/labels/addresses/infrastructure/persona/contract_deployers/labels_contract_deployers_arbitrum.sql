{{
    config(
        alias = alias('contract_deployers_arbitrum'),
        post_hook='{{ expose_spells(\'["arbitrum"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby", "hosuke"]\') }}'
    )
}}

SELECT distinct 'arbitrum'           AS blockchain
              , creation.`from`      AS address
              , 'Contract Deployer'  AS name
              , 'infrastructure'     AS category
              , 'hildobby'           AS contributor
              , 'query'              AS source
              , date('2023-03-03')   AS created_at
              , NOW()                AS updated_at
              , 'contract_deployers' AS model_name
              , 'persona'            AS label_type
FROM {{ source('arbitrum', 'creation_traces') }} creation
LEFT ANTI JOIN {{ source('arbitrum', 'creation_traces') }} anti_table
ON creation.from = anti_table.address