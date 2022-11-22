{{config(alias='flashbots_ethereum',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby"]\') }}')}}

SELECT DISTINCT array('ethereum') AS blockchain
, account_address AS address
, 'Flashbots User' AS name
, 'flashbots' AS category
, 'hildobby' AS contributor
, 'query' AS source
, date('2022-10-08') AS created_at
, NOW() AS modified_at
FROM {{ source('flashbots','arbitrages') }}