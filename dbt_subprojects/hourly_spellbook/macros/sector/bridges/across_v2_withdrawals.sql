{% macro across_v2_withdrawals(blockchain, events) %}

WITH across_id_mapping AS (
    SELECT id, blockchain
    FROM (VALUES
    (1, 'ethereum')
    , (10, 'optimism')
    , (137, 'polygon')
    , (42161, 'arbitrum')
    , (56, 'bnb')
    , (324, 'zksync')
    , (59144, 'linea')
    , (8453, 'base')
    , (7777777, 'zora')
    , (81457, 'blast')
    , (34443, 'mode')
    , (232, 'lens')
    , (57073, 'ink')
    , (1135, 'list')
    , (41455, 'aleph_zero')
    , (690, 'redstone')
    , (534352, 'scroll')
    , (1868, 'soneium')
    , (480, 'worldchain')
    , (130, 'unichain')
    ) AS x (id, blockchain)
    )
    
SELECT m.blockchain AS deposit_chain
, 'arbitrum' AS withdrawal_chain
, 'Across' AS bridge_name
, '2' AS bridge_version
, evt_block_date AS block_date
, evt_block_time AS block_time
, evt_block_number AS block_number
, outputAmount AS deposit_amount_raw
, CASE WHEN varbinary_substring(depositor,1, 12) = 0x000000000000000000000000 THEN varbinary_substring(depositor,13) ELSE depositor END AS sender
, CASE WHEN varbinary_substring(recipient,1, 12) = 0x000000000000000000000000 THEN varbinary_substring(recipient,13) ELSE recipient END AS recipient
, CASE WHEN varbinary_substring(outputToken,1, 12) = 0x000000000000000000000000 THEN varbinary_substring(outputToken,13) ELSE outputToken END AS withdrawal_token_address
, 'erc20' AS deposit_token_standard
, 'erc20' AS withdrawal_token_standard
, evt_tx_from AS tx_from
, evt_tx_hash AS tx_hash
, evt_index
, contract_address
, CAST(depositId_uint256 AS varchar) AS bridge_id
FROM ({{ events }}) d
LEFT JOIN across_id_mapping m ON d.originChainId=m.id

{% endmacro %}