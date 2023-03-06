{{config(alias='contract_deployers_fantom',
        post_hook='{{ expose_spells(\'["fantom"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby"]\') }}')}}

SELECT distinct 'fantom' AS blockchain
, creation.`from` AS address
, 'Contract Deployer' as name
, 'infrastructure' as category
, 'hildobby' as contributor
, 'query' AS source
, date('2023-03-03') AS created_at
, NOW() as modified_at
, 'contract_deployers' AS model_name
, 'persona' as label_type
FROM {{ source('fantom', 'creation_traces') }} creation
LEFT ANTI JOIN {{this}} anti_table ON creation.`from`=anti_table.`from`