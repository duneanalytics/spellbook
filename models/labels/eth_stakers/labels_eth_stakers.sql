{{config(alias='cex_ethereum',
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
    FROM {{ ref('ethereum_staking_entities')}}
    )

, unidentified_stakers AS (
    SELECT array('ethereum') AS blockchain
    , from AS address
    , 'Unidentified ETH staker' AS name
    , 'eth_staker' AS category
    , 'hildobby' AS contributor
    , 'query' AS source
    , timestamp('2023-01-18') AS created_at
    , NOW() AS updated_at
    FROM ethereum.traces
    WHERE to = '0x00000000219ab540356cbb839cbe05303d7705fa'
    AND success
    AND CAST(value AS double) > 0
    AND from NOT IN (SELECT address FROM identified_stakers)
    GROUP BY from
    )

SELECT * FROM identified_stakers
UNION ALL
SELECT * FROM unidentified_stakers