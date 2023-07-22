{{
    config(tags=['dunesql'],
        alias = alias('contract_deployers_bnb'),
        post_hook='{{ expose_spells(\'["bnb"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby", "hosuke"]\') }}'
    )
}}

SELECT distinct 'bnb'                AS blockchain
              , creation."from"      AS address
              , 'Contract Deployer'  AS name
              , 'infrastructure'     AS category
              , 'hildobby'           AS contributor
              , 'query'              AS source
              , date('2023-03-03')   AS created_at
              , NOW()                AS updated_at
              , 'contract_deployers' AS model_name
              , 'persona'            AS label_type
FROM {{ source('bnb', 'creation_traces') }} creation
LEFT JOIN {{ source('bnb', 'creation_traces') }} anti_table
    ON creation."from" = anti_table.address
WHERE anti_table.address is NULL