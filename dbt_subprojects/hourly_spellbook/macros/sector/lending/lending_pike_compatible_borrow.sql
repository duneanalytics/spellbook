{% macro lending_pike_compatible_borrow(blockchain, project, version, borrow_table) %}

WITH borrow_events AS (
    SELECT 
        b.contract_address,
        b.evt_tx_hash,
        b.evt_tx_from,
        b.evt_tx_to,
        b.evt_tx_index,
        b.evt_index,
        b.evt_block_time,
        b.evt_block_number,
        b.evt_block_date,
        b.accountBorrows,
        b.borrowAmount,
        b.borrower,
        b.onBehalfOf,
        b.totalBorrows,
        from_hex(json_extract_scalar(dm.setupParams, '$.underlying')) AS token_address
    FROM {{source(project ~ '_' ~ blockchain, borrow_table)}} b
    JOIN {{source(project ~ '_' ~ blockchain, 'factory_call_deploymarket')}} dm 
        ON b.contract_address = dm.output_pToken
    {% if is_incremental() %}
    WHERE {{ incremental_predicate('evt_block_time') }}
    {% endif %}
)

SELECT
    '{{ blockchain }}' as blockchain,
    '{{ project }}' as project,
    '{{ version }}' as version,
    'borrow' AS transaction_type,
    'borrow' AS loan_type,
    token_address,
    borrower,
    onBehalfOf as on_behalf_of,
    CAST(NULL AS varbinary) as repayer,
    CAST(NULL AS varbinary) as liquidator,
    borrowAmount as amount,
    cast(date_trunc('month', evt_block_time) as date) as block_month,
    evt_block_time as block_time,
    evt_block_number as block_number,
    contract_address as project_contract_address,
    evt_tx_hash as tx_hash,
    evt_index
FROM borrow_events

{% endmacro %}