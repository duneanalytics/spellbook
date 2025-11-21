{% macro lending_pike_compatible_supply(
    blockchain,
    project,
    version,
    evt_deposit_table,
    evt_withdraw_table,
    evt_liquidation_borrow_table,
    deploy_market_table
    ) 
%}

WITH deployed_markets AS (
  SELECT
    output_pToken AS ptoken_address,
    FROM_HEX(JSON_EXTRACT_SCALAR(setupParams, '$.underlying')) AS token_address,
    JSON_EXTRACT_SCALAR(setupParams, '$.symbol') AS symbol
  FROM {{ source(project ~ '_' ~ blockchain, deploy_market_table) }}
),

deposits AS (
    SELECT 
        '{{ blockchain }}' AS blockchain,
        'pike' AS project,
        '1' AS version,
        'deposit' AS transaction_type,
        dm.token_address,
        d.sender AS depositor,
        d.owner AS on_behalf_of,
        CAST(NULL AS varbinary) AS withdrawn_to,
        CAST(NULL AS varbinary) AS liquidator,
        d.assets AS amount,
        DATE_TRUNC('month', d.evt_block_time) AS block_month,
        d.evt_block_time AS block_time,
        d.evt_block_number AS block_number,
        d.contract_address AS project_contract_address,
        d.evt_tx_hash AS tx_hash,
        d.evt_index
    FROM {{ source(project ~ '_' ~ blockchain, evt_deposit_table) }} d
    JOIN deployed_markets dm ON d.contract_address = dm.ptoken_address
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('d.evt_block_time') }}
    {% endif %}
),

withdrawals AS (
    SELECT 
        '{{ blockchain }}' AS blockchain,
        'pike' AS project,
        '1' AS version,
        'withdraw' AS transaction_type,
        dm.token_address,
        w.sender AS depositor,
        w.owner AS on_behalf_of,
        w.receiver AS withdrawn_to,
        CAST(NULL AS varbinary) AS liquidator,
        -w.assets AS amount,
        DATE_TRUNC('month', w.evt_block_time) AS block_month,
        w.evt_block_time AS block_time,
        w.evt_block_number AS block_number,
        w.contract_address AS project_contract_address,
        w.evt_tx_hash AS tx_hash,
        w.evt_index
    FROM {{ source(project ~ '_' ~ blockchain, evt_withdraw_table) }} w
    JOIN deployed_markets dm ON w.contract_address = dm.ptoken_address
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('w.evt_block_time') }}
    {% endif %}
),

liquidations_supply AS (
    SELECT 
        '{{ blockchain }}' AS blockchain,
        'pike' AS project,
        '1' AS version,
        'liquidation_supply' AS transaction_type,
        dm.token_address,
        l.borrower AS depositor,
        l.borrower AS on_behalf_of,
        l.liquidator AS withdrawn_to,
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

SELECT * FROM deposits
UNION ALL
SELECT * FROM withdrawals
UNION ALL  
SELECT * FROM liquidations_supply

{% endmacro %}