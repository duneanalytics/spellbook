WITH token_time_range AS (
    SELECT
        blockchain,
        contract_address,
        symbol,
        MIN(timestamp) AS min_timestamp,
        MAX(timestamp) AS max_timestamp
    FROM {{ ref('prices_minute') }}
    GROUP BY blockchain, contract_address, symbol
),
all_minutes AS (
    SELECT DISTINCT timestamp
    FROM {{ ref('prices_minute') }}
)
SELECT
    am.timestamp AS missing_minute,
    ttr.blockchain,
    ttr.contract_address,
    ttr.symbol
FROM token_time_range ttr
CROSS JOIN all_minutes am
LEFT JOIN {{ ref('prices_minute') }} p ON am.timestamp = p.timestamp
    AND ttr.blockchain = p.blockchain
    AND ttr.contract_address = p.contract_address
    AND ttr.symbol = p.symbol
WHERE p.timestamp IS NULL
    AND am.timestamp BETWEEN ttr.min_timestamp AND ttr.max_timestamp

