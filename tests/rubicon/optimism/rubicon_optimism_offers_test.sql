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
        -- CASE
        --     WHEN test.block_date = actual.block_date THEN TRUE
        --     ELSE FALSE
        -- END AS block_date_test,

        -- test the block_time
        -- CASE
        --     WHEN test.block_time = actual.block_time THEN TRUE
        --     ELSE FALSE
        -- END AS block_time_test,

        -- test the block_number
        CASE
            WHEN test.block_number = actual.block_number THEN TRUE
            ELSE FALSE
        END AS block_number_test,

        -- test the tx_index
        CASE
            WHEN test.tx_index = actual.tx_index THEN TRUE
            ELSE FALSE
        END AS tx_index_test,

        -- test the evt_index
        CASE
            WHEN test.evt_index = actual.evt_index THEN TRUE
            ELSE FALSE
        END AS evt_index_test,

        -- test the maker
        CASE
            WHEN test.maker = actual.maker THEN TRUE
            ELSE FALSE
        END AS maker_test,

        -- test the sell_token_symbol
        CASE
            WHEN LOWER(test.sell_token_symbol) = LOWER(actual.sell_token_symbol) THEN TRUE
            ELSE FALSE
        END AS sell_token_symbol_test,

        -- test the buy_token_symbol
        CASE
            WHEN LOWER(test.buy_token_symbol) = LOWER(actual.buy_token_symbol) THEN TRUE
            ELSE FALSE
        END AS buy_token_symbol_test,

        -- test the sell_token_address
        CASE
            WHEN test.sell_token_address = actual.sell_token_address THEN TRUE
            ELSE FALSE
        END AS sell_token_address_test,

        -- test the buy_token_address
        CASE
            WHEN test.buy_token_address = actual.buy_token_address THEN TRUE
            ELSE FALSE
        END AS buy_token_address_test,

        -- test the token_pair
        CASE
            WHEN LOWER(test.token_pair) = LOWER(actual.token_pair) THEN TRUE
            ELSE FALSE
        END AS token_pair_test,

        -- test the sell_amount
        CASE 
            WHEN ROUND(test.sell_amount, 2) = ROUND(actual.sell_amount, 2) THEN TRUE
            ELSE FALSE
        END AS sell_amount_test,

        -- test the buy_amount
        CASE 
            WHEN ROUND(test.buy_amount, 2) = ROUND(actual.buy_amount, 2) THEN TRUE
            ELSE FALSE
        END AS buy_amount_test,

        -- test the sell_amount_raw
        -- CASE 
        --     WHEN LOWER(CAST(CAST(test.sell_amount_raw AS DECIMAL(38,0)) AS VARCHAR(100))) = LOWER(CAST(CAST(actual.sell_amount_raw AS DECIMAL(38,0)) AS VARCHAR(100))) THEN TRUE
        --     ELSE FALSE
        -- END AS sell_amount_raw_test,

        -- test the buy_amount_raw
        -- CASE 
        --     WHEN LOWER(CAST(CAST(test.buy_amount_raw AS DECIMAL(38,0)) AS VARCHAR(100))) = LOWER(CAST(CAST(actual.buy_amount_raw AS DECIMAL(38,0)) AS VARCHAR(100))) THEN TRUE
        --     ELSE FALSE
        -- END AS buy_amount_raw_test,

        -- test the sold_amount
        CASE 
            WHEN ROUND(test.sold_amount, 2) = ROUND(actual.sold_amount, 2) THEN TRUE
            ELSE FALSE
        END AS sold_amount_test,

        -- test the bought_amount
        CASE 
            WHEN ROUND(test.bought_amount, 2) = ROUND(actual.bought_amount, 2) THEN TRUE
            ELSE FALSE
        END AS bought_amount_test,

        -- test the sold_amount_raw
        -- CASE 
        --     WHEN LOWER(CAST(CAST(test.sold_amount_raw AS DECIMAL(38,0)) AS VARCHAR(100))) = LOWER(CAST(CAST(actual.sold_amount_raw AS DECIMAL(38,0)) AS VARCHAR(100))) THEN TRUE
        --     ELSE FALSE
        -- END AS sold_amount_raw_test,

        -- test the bought_amount_raw
        -- CASE 
        --     WHEN LOWER(CAST(CAST(test.bought_amount_raw AS DECIMAL(38,0)) AS VARCHAR(100))) = LOWER(CAST(CAST(actual.bought_amount_raw AS DECIMAL(38,0)) AS VARCHAR(100))) THEN TRUE
        --     ELSE FALSE
        -- END AS bought_amount_raw_test,

        -- test the sell_amount_usd
        CASE 
            WHEN ROUND(test.sell_amount_usd, 2) = ROUND(actual.sell_amount_usd, 2) THEN TRUE
            ELSE FALSE
        END AS sell_amount_usd_test,

        -- test the buy_amount_usd
        CASE 
            WHEN ROUND(test.buy_amount_usd, 2) = ROUND(actual.buy_amount_usd, 2) THEN TRUE
            ELSE FALSE
        END AS buy_amount_usd_test,

        -- test the sold_amount_usd
        CASE 
            WHEN ROUND(test.sold_amount_usd, 2) = ROUND(actual.sold_amount_usd, 2) THEN TRUE
            ELSE FALSE
        END AS sold_amount_usd_test,

        -- test the bought_amount_usd
        CASE 
            WHEN ROUND(test.bought_amount_usd, 2) = ROUND(actual.bought_amount_usd, 2) THEN TRUE
            ELSE FALSE
        END AS bought_amount_usd_test,

        -- test the gas_price
        CASE 
            WHEN test.gas_price = actual.gas_price THEN TRUE
            ELSE FALSE
        END AS gas_price_test,

        -- test the gas_used
        CASE 
            WHEN test.gas_used = actual.gas_used THEN TRUE
            ELSE FALSE
        END AS gas_used_test,

        -- test the l1_gas_price
        -- CASE 
        --     WHEN test.l1_gas_price = actual.l1_gas_price THEN TRUE
        --     ELSE FALSE
        -- END AS l1_gas_price_test,

        -- test the l1_gas_used
        -- CASE 
        --     WHEN test.l1_gas_used = actual.l1_gas_used THEN TRUE
        --     ELSE FALSE
        -- END AS l1_gas_used_test,

        -- test the l1_fee_scalar
        CASE 
            WHEN test.l1_fee_scalar = actual.l1_fee_scalar THEN TRUE
            ELSE FALSE
        END AS l1_fee_scalar_test,

        -- test the txn_cost_eth
        CASE 
            WHEN ROUND(test.txn_cost_eth, 2) = ROUND(actual.txn_cost_eth, 2) THEN TRUE
            ELSE FALSE
        END AS txn_cost_eth_test,

        -- test the eth_price
        CASE 
            WHEN ROUND(test.eth_price, 2) = ROUND(actual.eth_price, 2) THEN TRUE
            ELSE FALSE
        END AS eth_price_test,

        -- test the txn_cost_usd
        CASE 
            WHEN ROUND(test.txn_cost_usd, 2) = ROUND(actual.txn_cost_usd, 2) THEN TRUE
            ELSE FALSE
        END AS txn_cost_usd_test,

        -- test the project_contract_address
        CASE
            WHEN test.project_contract_address = actual.project_contract_address THEN TRUE
            ELSE FALSE
        END AS project_contract_address_test,

        -- test the tx_hash
        CASE
            WHEN test.tx_hash = actual.tx_hash THEN TRUE
            ELSE FALSE
        END AS tx_hash_test,

        -- test the tx_from
        CASE
            WHEN test.tx_from = actual.tx_from THEN TRUE
            ELSE FALSE
        END AS tx_from_test,

        -- test the tx_to
        CASE
            WHEN test.tx_to = actual.tx_to THEN TRUE
            ELSE FALSE
        END AS tx_to_test

    FROM
        {{ ref('rubicon_optimism_offers') }} AS actual
    INNER JOIN {{ ref('dex_offers_seed') }} AS test
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
    OR block_number_test = FALSE
    -- OR block_date_test = FALSE
    OR maker_test = FALSE
    OR sell_token_symbol_test = FALSE
    OR buy_token_symbol_test = FALSE
    OR sell_token_address_test = FALSE
    OR buy_token_address_test = FALSE
    OR token_pair_test = FALSE
    OR sell_amount_test = FALSE
    OR buy_amount_test = FALSE
    -- OR sell_amount_raw_test = FALSE
    -- OR buy_amount_raw_test = FALSE
    OR sold_amount_test = FALSE
    OR bought_amount_test = FALSE
    -- OR sold_amount_raw_test = FALSE
    -- OR bought_amount_raw_test = FALSE
    OR sell_amount_usd_test = FALSE
    OR buy_amount_usd_test = FALSE
    OR sold_amount_usd_test = FALSE
    OR bought_amount_usd_test = FALSE
    OR gas_price_test = FALSE
    OR gas_used_test = FALSE
    -- OR l1_gas_price_test = FALSE
    -- OR l1_gas_used_test = FALSE
    OR l1_fee_scalar_test = FALSE
    OR txn_cost_eth_test = FALSE
    OR eth_price_test = FALSE
    OR txn_cost_usd_test = FALSE
    OR project_contract_address_test = FALSE
    OR tx_hash_test = FALSE
    OR tx_from_test = FALSE
    OR tx_to_test = FALSE