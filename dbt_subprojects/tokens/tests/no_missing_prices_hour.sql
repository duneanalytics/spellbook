{% test no_missing_price_hours(model) %}

    WITH token_time_range AS (
        SELECT
            blockchain,
            contract_address,
            symbol,
            MIN(timestamp) AS min_timestamp,
            MAX(timestamp) AS max_timestamp
        FROM {{ model }}
        GROUP BY blockchain, contract_address, symbol
    ),
    all_hours AS (
        SELECT DISTINCT timestamp
        FROM {{ model }}
    )
    SELECT
        ah.timestamp AS missing_hour,
        ttr.blockchain,
        ttr.contract_address,
        ttr.symbol
    FROM token_time_range ttr
    CROSS JOIN all_hours ah
    LEFT JOIN {{ model }} p ON ah.timestamp = p.timestamp
        AND ttr.blockchain = p.blockchain
        AND ttr.contract_address = p.contract_address
        AND ttr.symbol = p.symbol
    WHERE p.timestamp IS NULL
        AND ah.timestamp BETWEEN ttr.min_timestamp AND ttr.max_timestamp

{% endtest %}
