{% macro lending_pike_v1_compatible_borrow(blockchain, project, version) %}

WITH deploy_market_data AS (
    SELECT 
        call_block_time AS block_time,
        call_tx_hash AS tx_hash,
        call_block_number AS block_number,
        contract_address AS project_contract_address,
        '{{ blockchain }}' AS blockchain,
        '{{ project }}' AS project,
        '{{ version }}' AS version,
        'borrow' AS transaction_type,
        json_extract_scalar(setupParams, '$.underlying') AS token_address,  -- Extract token address from JSON
        json_extract_scalar(setupParams, '$.borrower') AS borrower,         -- Extract borrower address from JSON
        json_extract_scalar(setupParams, '$.on_behalf_of') AS on_behalf_of, -- Extract on_behalf_of address from JSON
        json_extract_scalar(setupParams, '$.amount') AS amount,             -- Extract borrowed amount from JSON
        NULL AS repayer,
        NULL AS liquidator,
        cast(date_trunc('month', call_block_time) as date) as block_month
    FROM blockchain.calls
    WHERE contract_address IN (SELECT pike_contract FROM pike_lending_markets)
      AND function_name = 'deployMarket'
),

borrow_events AS (
    SELECT 
        evt_block_time AS block_time,
        evt_tx_hash AS tx_hash,
        evt_index AS evt_index,
        contract_address AS project_contract_address,
        '{{ blockchain }}' AS blockchain,
        '{{ project }}' AS project,
        '{{ version }}' AS version,
        'borrow' AS transaction_type,
        evt_indexed_parameters[0] AS borrower,
        evt_indexed_parameters[1] AS on_behalf_of,
        evt_parameters[0]::NUMERIC AS amount,        -- Borrowed amount
        evt_parameters[1]::NUMERIC AS account_borrows, -- Account's total borrow balance
        evt_parameters[2]::NUMERIC AS total_borrows,   -- Total borrows in the market
        evt_parameters[0]::NUMERIC AS amount_usd,    -- Assuming amount is in USD (or needs conversion)
        token_address, 
        block_number,
        block_month
    FROM blockchain.logs
    WHERE contract_address IN (SELECT pike_contract FROM pike_lending_markets)
      AND event_name = 'Borrow'
)

SELECT 
    blockchain,
    project,
    version,
    transaction_type,
    'borrow' AS loan_type,
    token_address,
    borrower,
    on_behalf_of,
    NULL AS repayer,
    NULL AS liquidator,
    amount,
    block_month,
    block_time,
    block_number,
    project_contract_address,
    tx_hash,
    evt_index
FROM deploy_market_data

UNION ALL

SELECT 
    blockchain,
    project,
    version,
    transaction_type,
    'borrow' AS loan_type,
    token_address,
    borrower,
    on_behalf_of,
    NULL AS repayer,
    NULL AS liquidator,
    amount,
    block_month,
    block_time,
    block_number,
    project_contract_address,
    tx_hash,
    evt_index
FROM borrow_events

{% endmacro %}