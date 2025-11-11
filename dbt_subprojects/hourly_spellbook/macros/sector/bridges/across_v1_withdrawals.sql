{% macro across_v1_withdrawals(blockchain, events) %}

WITH events AS (
    SELECT d.originChainId AS deposit_chain_id
    , m.blockchain AS deposit_chain
    , d.evt_block_date AS block_date
    , d.evt_block_time AS block_time
    , d.evt_block_number AS block_number
    , d.fillamount AS withdrawal_amount_raw
    , depositor AS sender
    , recipient AS recipient
    , destinationToken AS withdrawal_token_address
    , 'erc20' AS deposit_token_standard
    , 'erc20' AS withdrawal_token_standard
    , d.evt_tx_from AS tx_from
    , d.evt_tx_hash AS tx_hash
    , d.evt_index
    , d.contract_address
    , CAST(d.depositId AS varchar) AS bridge_transfer_id
    , ROW_NUMBER() OVER (PARTITION BY d.originChainId, d.depositId ORDER BY d.evt_block_number, d.evt_index) AS rn
    FROM ({{ events }}) d
    LEFT JOIN {{ ref('bridges_across_chain_indexes') }} m ON d.originChainId=m.id
    WHERE CAST(d.fillamount AS double) > 0.5*CAST(d.amount AS double)
    )

SELECT deposit_chain_id
, deposit_chain
, '{{blockchain}}' AS withdrawal_chain
, 'Across' AS bridge_name
, '1' AS bridge_version
, block_date
, block_time
, block_number
, withdrawal_amount_raw
, sender
, recipient
, withdrawal_token_address
, deposit_token_standard
, withdrawal_token_standard
, tx_from
, tx_hash
, evt_index
, contract_address
, bridge_transfer_id
FROM events
WHERE rn = 1

{% endmacro %}