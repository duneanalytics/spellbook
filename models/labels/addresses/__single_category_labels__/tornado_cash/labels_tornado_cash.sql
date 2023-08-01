{{config(
        tags=['dunesql'],
        alias = alias('tornado_cash'),
        post_hook='{{ expose_spells(\'["ethereum", "arbitrum","bnb","avalanche_c","optimism","gnosis"]\',
                                    "sector",
                                    "labels",
                                    \'["soispoke"]\') }}'
)}}

WITH tornado_addresses AS (
SELECT
    lower(blockchain) as blockchain,
    tx_hash,
    depositor AS address,
    'Depositor' as name
FROM {{ ref('tornado_cash_deposits') }}
UNION
SELECT
    lower(blockchain) as blockchain,
    tx_hash,
    recipient AS address,
    'Recipient' as name
FROM {{ ref('tornado_cash_withdrawals') }}
)

SELECT
    blockchain,
    address,
    'Tornado Cash ' || array_join(ARRAY_AGG(DISTINCT name),' and ') AS name,
    'tornado_cash' AS category,
    'soispoke' AS contributor,
    'query' AS source,
    timestamp '2022-10-01' as created_at,
    now() as updated_at,
    'tornado_cash' AS model_name,
    'persona' AS label_type
FROM tornado_addresses
GROUP BY address, blockchain