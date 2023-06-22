WITH unit_test AS (
    SELECT
        -- test the blockchain 
        CASE 
            WHEN LOWER(test.blockchain) = LOWER(actual.blockchain) THEN TRUE
            ELSE FALSE
        END AS blockchain_test,

        -- test the project 
        CASE 
            WHEN LOWER(test.project) = LOWER(actual.project) THEN TRUE
            ELSE FALSE
        END AS project_test,

        -- test the version 
        CASE 
            WHEN test.version = actual.version THEN TRUE
            ELSE FALSE
        END AS version_test,

        -- test the block_date
        CASE
            WHEN test.block_date = actual.block_date THEN TRUE
            ELSE FALSE
        END AS block_date_test,

        -- test the block_time
        CASE
            WHEN test.block_time = actual.block_time THEN TRUE
            ELSE FALSE
        END AS block_time_test,

        -- test the token_bought_symbol
        CASE
            WHEN LOWER(test.token_bought_symbol) = LOWER(actual.token_bought_symbol) THEN TRUE
            ELSE FALSE
        END AS token_bought_symbol_test,

        -- test the token_sold_symbol
        CASE
            WHEN LOWER(test.token_sold_symbol) = LOWER(actual.token_sold_symbol) THEN TRUE
            ELSE FALSE
        END AS token_sold_symbol_test,

        -- test the token_pair
        CASE
            WHEN LOWER(test.token_pair) = LOWER(actual.token_pair) THEN TRUE
            ELSE FALSE
        END AS token_pair_test,

        -- test the token_bought_amount 
        CASE
            WHEN ROUND(test.token_bought_amount, 4) = ROUND(actual.token_bought_amount, 4) THEN TRUE
            ELSE FALSE
        END AS token_bought_amount_test,

        -- test the token_sold_amount
        CASE
            WHEN ROUND(test.token_sold_amount, 4) = ROUND(actual.token_sold_amount, 4) THEN TRUE
            ELSE FALSE
        END AS token_sold_amount_test,

        -- test the amount_usd
        CASE
            WHEN ROUND(test.amount_usd, 4) = ROUND(actual.amount_usd, 4) THEN TRUE
            ELSE FALSE
        END AS amount_usd_test,

        -- test the token_bought_address
        CASE
            WHEN LOWER(test.token_bought_address) = LOWER(actual.token_bought_address) THEN TRUE
            ELSE FALSE
        END AS token_bought_address_test,

        -- test the token_sold_address
        CASE
            WHEN LOWER(test.token_sold_address) = LOWER(actual.token_sold_address) THEN TRUE
            ELSE FALSE
        END AS token_sold_address_test,

        -- test the taker
        CASE
            WHEN LOWER(test.taker) = LOWER(actual.taker) THEN TRUE
            ELSE FALSE
        END AS taker_test,

        -- test the maker
        CASE
            WHEN LOWER(test.maker) = LOWER(actual.maker) THEN TRUE
            ELSE FALSE
        END AS maker_test,

        -- test the project_contract_address
        CASE
            WHEN LOWER(test.project_contract_address) = LOWER(actual.project_contract_address) THEN TRUE
            ELSE FALSE
        END AS project_contract_address_test,

        -- test the tx_hash
        CASE
            WHEN LOWER(test.tx_hash) = LOWER(actual.tx_hash) THEN TRUE
            ELSE FALSE
        END AS tx_hash_test,

        -- test the tx_from
        CASE
            WHEN LOWER(test.tx_from) = LOWER(actual.tx_from) THEN TRUE
            ELSE FALSE
        END AS tx_from_test,

        -- test the tx_to
        CASE
            WHEN LOWER(test.tx_to) = LOWER(actual.tx_to) THEN TRUE
            ELSE FALSE
        END AS tx_to_test,

        -- test the evt_index
        CASE
            WHEN test.evt_index = actual.evt_index THEN TRUE
            ELSE FALSE
        END AS evt_index_test



    FROM
        {{ ref('rubicon_arbitrum_trades') }} AS actual
    INNER JOIN {{ ref('rubicon_arbitrum_trades_seed') }} AS test
        ON actual.block_date = test.block_date
        AND actual.blockchain = test.blockchain
        AND actual.project = test.project
        AND actual.version = test.version
        AND actual.tx_hash = test.tx_hash
        AND actual.evt_index = test.evt_index

)

-- Return any FALSE results
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
    OR amount_usd_test = FALSE
    OR token_bought_address_test = FALSE
    OR token_sold_address_test = FALSE
    OR taker_test = FALSE
    OR maker_test = FALSE
    OR project_contract_address_test = FALSE
    OR tx_hash_test = FALSE
    OR tx_from_test = FALSE
    OR tx_to_test = FALSE
    OR evt_index_test = FALSE