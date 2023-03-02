WITH unit_test AS (
    SELECT
        CASE
            WHEN LOWER(test.blockchain) = LOWER(actual.blockchain) THEN TRUE
            ELSE FALSE
        END AS blockchain_test,
        CASE
            WHEN LOWER(test.project) = LOWER(actual.project) THEN TRUE
            ELSE FALSE
        END AS project_test,
        CASE
            WHEN test.version = actual.version THEN TRUE
            ELSE FALSE
        END AS version_test,
        CASE
            WHEN test.block_date = actual.block_date THEN TRUE
            ELSE FALSE
        END AS block_date_test,
        CASE
            WHEN test.block_time = actual.block_time THEN TRUE
            ELSE FALSE
        END AS block_time_test,
        CASE
            WHEN LOWER(test.token_bought_symbol) = LOWER(actual.token_bought_symbol) THEN TRUE
            ELSE FALSE
        END AS token_bought_symbol_test,
        CASE
            WHEN LOWER(test.token_sold_symbol) = LOWER(actual.token_sold_symbol) THEN TRUE
            ELSE FALSE
        END AS token_sold_symbol_test,
        CASE
            WHEN LOWER(test.token_pair) = LOWER(actual.token_pair) THEN TRUE
            ELSE FALSE
        END AS token_pair_test,
        CASE
            WHEN ROUND(test.token_bought_amount, 6) = ROUND(actual.token_bought_amount, 6) THEN TRUE
            ELSE FALSE
        END AS token_bought_amount_test,
        CASE
            WHEN ROUND(test.token_sold_amount, 6) = ROUND(actual.token_sold_amount, 6) THEN TRUE
            ELSE FALSE
        END AS token_sold_amount_test,
        CASE
            WHEN test.token_bought_amount_raw = actual.token_bought_amount_raw THEN TRUE
            ELSE FALSE
        END AS token_bought_amount_raw_test,
        CASE
            WHEN test.token_sold_amount_raw = actual.token_sold_amount_raw THEN TRUE
            ELSE FALSE
        END AS token_sold_amount_raw_test,
        CASE
            WHEN ROUND(test.amount_usd, 2) = ROUND(actual.amount_usd, 2) THEN TRUE
            ELSE FALSE
        END AS amount_usd_test,
        CASE
            WHEN LOWER(test.token_bought_address) = LOWER(actual.token_bought_address) THEN TRUE
            ELSE FALSE
        END AS token_bought_address_test,
        CASE
            WHEN LOWER(test.token_sold_address) = LOWER(actual.token_sold_address) THEN TRUE
            ELSE FALSE
        END AS token_sold_address_test,
        CASE
            WHEN LOWER(test.taker) = LOWER(actual.taker) THEN TRUE
            ELSE FALSE
        END AS taker_test,
--         CASE
--             WHEN LOWER(test.maker) = LOWER(actual.maker) THEN TRUE
--             ELSE FALSE
--         END AS maker_test,
        CASE
            WHEN LOWER(test.project_contract_address) = LOWER(actual.project_contract_address) THEN TRUE
            ELSE FALSE
        END AS project_contract_address_test,
        CASE
            WHEN LOWER(test.tx_hash) = LOWER(actual.tx_hash) THEN TRUE
            ELSE FALSE
        END AS tx_hash_test,
        CASE
            WHEN LOWER(test.tx_from) = LOWER(actual.tx_from) THEN TRUE
            ELSE FALSE
        END AS tx_from_test,
        CASE
            WHEN LOWER(test.tx_to) = LOWER(actual.tx_to) THEN TRUE
            ELSE FALSE
        END AS tx_to_test

    FROM {{ ref('clipper_arbitrum_trades') }} AS actual
        INNER JOIN {{ ref('clipper_arbitrum_trades_test_data') }} AS test
        ON LOWER(
            actual.tx_hash
        ) = LOWER(
            test.tx_hash
        )
)

-- Loading all columns from unit_test, we return any FALSE results

SELECT
    *
FROM
    unit_test
WHERE
    blockchain_test = FALSE
    OR project_test = FALSE
    OR version_test = FALSE
    OR block_date_test = FALSE
    OR block_time_test = FALSE
    OR token_bought_symbol_test = FALSE
    OR token_sold_symbol_test = FALSE
    OR token_pair_test = FALSE
    OR token_bought_amount_test = FALSE
    OR token_sold_amount_test = FALSE
    OR token_bought_amount_raw_test = FALSE
    OR token_sold_amount_raw_test = FALSE
    OR amount_usd_test = FALSE
    OR token_bought_address_test = FALSE
    OR token_sold_address_test = FALSE
    OR taker_test = FALSE
--    OR maker_test = FALSE
    OR project_contract_address_test = FALSE
    OR tx_hash_test = FALSE
    OR tx_from_test = FALSE
    OR tx_to_test = FALSE
