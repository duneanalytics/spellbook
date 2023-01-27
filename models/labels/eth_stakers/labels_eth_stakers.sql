{{config(alias='eth_stakers',
        post_hook='{{ expose_spells(\'["ethereum"]\',
                                    "sector",
                                    "labels",
                                    \'["hildobby"]\') }}')}}

WITH identified_stakers AS (
    SELECT array('ethereum') AS blockchain
    , address
    , entity AS name
    , 'eth_staker' AS category
    , 'hildobby' AS contributor
    , 'query' AS source
    , timestamp('2023-01-18') AS created_at
    , NOW() AS updated_at
    FROM {{ ref('staking_ethereum_entities')}}
    )

, unidentified_stakers AS (
    SELECT array('ethereum') AS blockchain
    , et.from AS address
    , 'Unidentified ETH staker' AS name
    , 'eth_staker' AS category
    , 'hildobby' AS contributor
    , 'query' AS source
    , timestamp('2023-01-18') AS created_at
    , NOW() AS updated_at
    FROM {{ source('ethereum', 'traces') }} et
    LEFT ANTI JOIN identified_stakers is
        ON et.from = is.address
    WHERE et.to = '0x00000000219ab540356cbb839cbe05303d7705fa'
    AND et.success
    AND CAST(et.value AS double) > 0
    GROUP BY et.from
    )

SELECT * FROM identified_stakers
UNION ALL
SELECT * FROM unidentified_stakers