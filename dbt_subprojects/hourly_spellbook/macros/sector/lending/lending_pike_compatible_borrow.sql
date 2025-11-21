{% macro lending_pike_compatible_borrow(
    blockchain,
    project,
    version,
    evt_borrow_table,
    evt_repay_table,
    evt_liquidation_borrow_table,
    deploy_market_table
    ) 
%}

WITH deployed_markets AS (
  SELECT
    output_pToken AS ptoken_address,
    from_hex(JSON_EXTRACT_SCALAR(setupParams, '$.underlying')) AS token_address,
    JSON_EXTRACT_SCALAR(setupParams, '$.symbol') AS symbol
  FROM {{ source(project ~ '_' ~ blockchain, deploy_market_table) }}
),

borrows AS (
    SELECT
        '{{ blockchain }}' AS blockchain,
        'pike' AS project,
        '1' AS version,
        'borrow' AS transaction_type,
        'borrow' AS loan_type,
        dm.token_address,
        b.borrower,
        COALESCE(b.onBehalfOf, b.borrower) AS on_behalf_of,
        CAST(NULL AS varbinary) AS repayer,
        CAST(NULL AS varbinary) AS liquidator,
        b.borrowAmount AS amount,
        DATE_TRUNC('month', b.evt_block_time) AS block_month,
        b.evt_block_time AS block_time,
        b.evt_block_number AS block_number,
        b.contract_address AS project_contract_address,
        b.evt_tx_hash AS tx_hash,
        b.evt_index
    FROM {{ source(project ~ '_' ~ blockchain, evt_borrow_table) }} b
    JOIN deployed_markets dm ON b.contract_address = dm.ptoken_address
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('b.evt_block_time') }}
    {% endif %}
),

repays AS (
    SELECT
        '{{ blockchain }}' AS blockchain,
        'pike' AS project,
        '1' AS version,
        'repay' AS transaction_type,
        'borrow' AS loan_type,
        dm.token_address,
        r.onBehalfOf AS borrower,
        r.onBehalfOf AS on_behalf_of,
        r.payer AS repayer,
        CAST(NULL AS varbinary) AS liquidator,
        -r.repayAmount AS amount,
        DATE_TRUNC('month', r.evt_block_time) AS block_month,
        r.evt_block_time AS block_time,
        r.evt_block_number AS block_number,
        r.contract_address AS project_contract_address,
        r.evt_tx_hash AS tx_hash,
        r.evt_index
    FROM {{ source(project ~ '_' ~ blockchain, evt_repay_table) }} r
    JOIN deployed_markets dm ON r.contract_address = dm.ptoken_address
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('r.evt_block_time') }}
    {% endif %}
),

liquidations_borrow AS (
    SELECT
        '{{ blockchain }}' AS blockchain,
        'pike' AS project,
        '1' AS version,
        'liquidation_borrow' AS transaction_type,
        'borrow' AS loan_type,
        dm.token_address,
        l.borrower,
        l.borrower AS on_behalf_of,
        l.liquidator AS repayer,
        l.liquidator,
        -l.repayAmount AS amount,
        DATE_TRUNC('month', l.evt_block_time) AS block_month,
        l.evt_block_time AS block_time,
        l.evt_block_number AS block_number,
        l.pTokenCollateral AS project_contract_address,
        l.evt_tx_hash AS tx_hash,
        l.evt_index
    FROM {{ source(project ~ '_' ~ blockchain, evt_liquidation_borrow_table) }} l
    JOIN deployed_markets dm ON l.pTokenCollateral = dm.ptoken_address
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('l.evt_block_time') }}
    {% endif %}
)

SELECT * FROM borrows
UNION ALL
SELECT * FROM repays
UNION ALL
SELECT * FROM liquidations_borrow

{% endmacro %}