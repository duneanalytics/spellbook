{{config(alias = alias('flashbots_ethereum'),
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby"]\') }}')}}

SELECT DISTINCT 'ethereum' AS blockchain
, account_address AS address
, 'Flashbots User' AS name
, 'infrastructure' AS category
, 'hildobby' AS contributor
, 'query' AS source
, date('2022-10-08') AS created_at
, NOW() AS updated_at
, 'flashbots' AS model_name
, 'persona' as label_type
FROM {{ source('flashbots','arbitrages') }}