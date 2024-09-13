{{ config
(
    alias = 'first_funded_by',
    schema = 'addresses_events_crosschain',
    post_hook='{{ expose_spells(\'["arbitrum", "avalanche_c", "bnb", "ethereum", "fantom", "gnosis", "optimism", "polygon", "celo", "zora", "base", "scroll"]\',
                                    "sector",
                                    "addresses_events",
                                    \'["hildobby"]\') }}'
)
}}

SELECT MIN_BY(blockchain, block_time) AS blockchain
, address
, MIN_BY(first_funded_by, block_time) AS first_funded_by
, array_distinct(array_agg(blockchain)) AS chains_funded_on
, MIN_BY(first_funding_executed_by, block_time) AS first_funding_executed_by
, MIN(block_time) AS block_time
, MIN(block_number) AS block_number
, MIN_BY(tx_hash, block_time) AS tx_hash
, MIN_BY(tx_index, block_time) AS tx_index
FROM {{ ref('addresses_events_first_funded_by') }}
GROUP BY address