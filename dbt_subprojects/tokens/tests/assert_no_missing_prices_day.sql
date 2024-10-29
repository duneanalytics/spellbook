WITH token_time_range AS (
    SELECT
        blockchain,
        contract_address,
        symbol,
        DATE(MIN(timestamp)) AS min_date,
        DATE(MAX(timestamp)) AS max_date
    FROM {{ ref('prices_day') }}
    GROUP BY blockchain, contract_address, symbol
),
all_days AS (
    SELECT DISTINCT DATE(timestamp) AS date
    FROM {{ ref('prices_day') }}
)
SELECT
    ad.date AS missing_date,
    ttr.blockchain,
    ttr.contract_address,
    ttr.symbol
FROM token_time_range ttr
CROSS JOIN all_days ad
LEFT JOIN {{ ref('prices_day') }} p ON ad.date = DATE(p.timestamp)
    AND ttr.blockchain = p.blockchain
    AND ttr.contract_address = p.contract_address
    AND ttr.symbol = p.symbol
WHERE p.timestamp IS NULL
    AND ad.date BETWEEN ttr.min_date AND ttr.max_date

