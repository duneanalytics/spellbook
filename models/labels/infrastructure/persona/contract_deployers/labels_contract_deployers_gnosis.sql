{{config(alias='contract_deployers_gnosis',
        post_hook='{{ expose_spells(\'["gnosis"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby"]\') }}')}}

SELECT distinct 'gnosis' AS blockchain
, creation."from" AS address
, 'Contract Deployer' as name
, 'infrastructure' as category
, 'hildobby' as contributor
, 'query' AS source
, date('2023-03-03') AS created_at
, NOW() as modified_at
, 'contract_deployers' AS model_name
, 'persona' as label_type
FROM {{ source('gnosis', 'creation_traces') }} creation
LEFT ANTI JOIN {{this}} anti ON creation."from"=anti."from"