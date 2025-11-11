{% macro across_v3_withdrawals(blockchain, events) %}

WITH ranked AS (
    SELECT d.originChainId AS deposit_chain_id
    , m.blockchain AS deposit_chain
    , '{{blockchain}}' AS withdrawal_chain
    , 'Across' AS bridge_name
    , '3' AS bridge_version
    , d.evt_block_date AS block_date
    , d.evt_block_time AS block_time
    , d.evt_block_number AS block_number
    , COALESCE(CAST(json_extract_scalar(d.relayExecutionInfo, '$.updatedOutputAmount') AS BIGINT), d.outputAmount) AS withdrawal_amount_raw
    , CASE WHEN varbinary_substring(d.depositor,1, 12) = 0x000000000000000000000000 THEN varbinary_substring(d.depositor,13) ELSE d.depositor END AS sender
    , CASE WHEN varbinary_substring(d.recipient,1, 12) = 0x000000000000000000000000 THEN varbinary_substring(d.recipient,13) ELSE d.recipient END AS recipient
    , CASE WHEN varbinary_substring(d.outputToken,1, 12) = 0x000000000000000000000000 THEN varbinary_substring(d.outputToken,13) ELSE d.outputToken END AS withdrawal_token_address
    , 'erc20' AS deposit_token_standard
    , 'erc20' AS withdrawal_token_standard
    , d.evt_tx_from AS tx_from
    , d.evt_tx_hash AS tx_hash
    , d.evt_index
    , d.contract_address
    , CAST(d.depositId AS varchar) AS bridge_transfer_id
    , ROW_NUMBER() OVER (PARTITION BY m.blockchain, d.depositId ORDER BY d.evt_block_number DESC, d.evt_index DESC) AS rn
    FROM ({{ events }}) d
    LEFT JOIN {{ ref('bridges_across_chain_indexes') }} m ON d.originChainId=m.id
    )

SELECT deposit_chain_id
, deposit_chain
, withdrawal_chain
, bridge_name
, bridge_version
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
FROM ranked
WHERE rn = 1

{% endmacro %}