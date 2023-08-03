{{
    config(tags=['dunesql'],
        alias = alias('contract_deployers_avalanche_c'),
        post_hook='{{ expose_spells(\'["avalanche_c"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby", "hosuke"]\') }}'
    )
}}

SELECT distinct 'avalanche_c'        AS blockchain
              , creation."from"      AS address
              , 'Contract Deployer'  as name
              , 'infrastructure'     as category
              , 'hildobby'           as contributor
              , 'query'              AS source
              , date('2023-03-03')   AS created_at
              , NOW()                as updated_at
              , 'contract_deployers' AS model_name
              , 'persona'            as label_type
FROM {{ source('avalanche_c', 'creation_traces') }} creation
LEFT JOIN {{ source('avalanche_c', 'creation_traces') }} anti_table
    ON creation."from" = anti_table.address
WHERE anti_table.address is NULL