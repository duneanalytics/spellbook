{% macro lending_pike_v1_compatible_supply(blockchain, protocol) %}

WITH pike_supply_events AS (
    SELECT 
        deposit.evt_block_time AS block_time,
        deposit.evt_tx_hash AS tx_hash,
        deposit.evt_index AS evt_index,
        deposit.contract_address AS project_contract_address,
        deposit.event_name,

        deposit.evt_indexed_parameters[0] AS depositor,  
        deposit.evt_indexed_parameters[1] AS on_behalf_of,  
        deposit.evt_parameters[0]::NUMERIC AS amount, 
        deposit.evt_parameters[1]::NUMERIC AS shares, 

        json_extract_scalar(deploy.underlying, '$.address') AS token_address,
        json_extract_scalar(deploy.underlying, '$.symbol') AS symbol,

        '{{ blockchain }}' AS blockchain,
        '{{ protocol }}' AS project,
        '1' AS version, 
        'supply' AS transaction_type,

        DATE_TRUNC('month', deposit.evt_block_time) AS block_month,
        deposit.evt_block_time AS block_time,
        deposit.evt_block_number AS block_number
    FROM {{ blockchain }}.{{ protocol }}_evt_Deposit deposit
    JOIN {{ blockchain }}.{{ protocol }}_Factory_call_deployMarket deploy
        ON deposit.contract_address = deploy.output_pToken
)

SELECT 
    blockchain,
    project,
    version,
    transaction_type,
    symbol,
    token_address,
    depositor,
    on_behalf_of,
    NULL AS withdrawn_to,
    NULL AS liquidator,
    amount,
    shares AS pTokens_minted,
    block_month,
    block_time,
    block_number,
    project_contract_address,
    tx_hash,
    evt_index
FROM pike_supply_events;

{% endmacro %}
