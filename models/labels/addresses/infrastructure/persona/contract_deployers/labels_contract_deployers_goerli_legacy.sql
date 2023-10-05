{{
    config(
	tags=['legacy'],
	
        alias = alias('contract_deployers_goerli', legacy_model=True),
        post_hook='{{ expose_spells(\'["goerli"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby", "hosuke"]\') }}'
    )
}}

SELECT distinct 'goerli'             AS blockchain
              , creation.`from`      AS address
              , 'Contract Deployer'  AS name
              , 'infrastructure'     AS category
              , 'hildobby'           AS contributor
              , 'query'              AS source
              , date('2023-03-03')   AS created_at
              , NOW()                AS updated_at
              , 'contract_deployers' AS model_name
              , 'persona'            AS label_type
FROM {{ source('goerli', 'creation_traces') }} creation
LEFT ANTI JOIN {{ source('goerli', 'creation_traces') }} anti_table
ON creation.from = anti_table.address