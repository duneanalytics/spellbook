{{config(
        alias = 'eth_stakers',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby"]\') }}')}}

WITH identified_stakers AS (
    SELECT 'ethereum' AS blockchain
    , depositor_address as address
    , entity AS name
    , 'infrastructure' AS category
    , 'hildobby' AS contributor
    , 'query' AS source
    , TIMESTAMP '2023-01-18'  AS created_at
    , NOW() AS updated_at
    , 'eth_stakers' AS model_name
    , 'identifier' as label_type
    FROM {{ ref('staking_ethereum_entities') }}
    )

, unidentified_stakers AS (
    SELECT 'ethereum' AS blockchain
    , et."from" AS address
    , 'Unidentified ETH staker' AS name
    , 'infrastructure' AS category
    , 'hildobby' AS contributor
    , 'query' AS source
    , TIMESTAMP '2023-01-18'  AS created_at
    , NOW() AS updated_at
    , 'eth_stakers' AS model_name
    , 'identifier' as label_type
    FROM {{ source('ethereum', 'traces') }} et
    LEFT JOIN identified_stakers idst
        ON et."from" = idst.address
    WHERE et.to = 0x00000000219ab540356cbb839cbe05303d7705fa
        AND idst.address IS NULL
        AND et.success
        AND CAST(et.value AS double) > 0
    GROUP BY et."from"
    )

SELECT * FROM identified_stakers
UNION ALL
SELECT * FROM unidentified_stakers