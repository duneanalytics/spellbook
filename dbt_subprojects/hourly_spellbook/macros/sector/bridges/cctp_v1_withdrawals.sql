{% macro cctp_v1_withdrawals(blockchain) %}

WITH cctp_id_mapping AS (
    SELECT id, blockchain
    FROM (VALUES
    (0, 'ethereum')
    , (1, 'avalanche_c')
    , (2, 'optimism')
    , (3, 'arbitrum')
    , (4, 'noble')
    , (5, 'solana')
    , (6, 'base')
    , (7, 'polygon')
    , (8, 'sui')
    , (9, 'aptos')
    , (10, 'unichain')
    , (11, 'linea')
    , (12, 'codex')
    , (13, 'sonic')
    , (14, 'worldchain')
    ) AS x (id, blockchain)
    )

, messages AS (
    SELECT evt_block_number AS block_number
    , evt_tx_hash AS tx_hash
    , CASE WHEN varbinary_substring(sender,1, 12) = 0x000000000000000000000000 THEN varbinary_substring(sender,13) ELSE sender END AS sender
    , nonce
    , evt_index
    , sourceDomain
    , ROW_NUMBER() OVER (PARTITION BY evt_block_number, evt_tx_hash ORDER BY evt_index) AS join_index
    FROM {{ source('circle_'  + blockchain, 'messagetransmitter_evt_messagereceived')}}
    )

, withdrawals AS (
    SELECT w.evt_block_date AS block_date
    , w.evt_block_time AS block_time
    , w.evt_block_number AS block_number
    , w.amount AS withdrawal_amount_raw
    , w.mintRecipient AS recipient
    , w.mintToken AS withdrawal_token_address
    , w.evt_tx_from AS tx_from
    , w.evt_tx_hash AS tx_hash
    , w.evt_index
    , w.contract_address
    , ROW_NUMBER() OVER (PARTITION BY evt_block_number, evt_tx_hash ORDER BY evt_index) AS join_index
    FROM {{ source('circle_'  + blockchain, 'tokenmessenger_evt_mintandwithdraw')}} w
    )

, closest_messages AS (
    SELECT w.block_number, w.join_index, w.tx_hash, w.evt_index AS withdrawal_evt_index,
           m.evt_index AS message_evt_index, m.sender, m.nonce, m.sourceDomain,
           ROW_NUMBER() OVER (
               PARTITION BY w.block_number, w.join_index, w.tx_hash, w.evt_index 
               ORDER BY m.evt_index ASC
           ) AS rn
    FROM withdrawals w
    INNER JOIN messages m ON w.block_number = m.block_number
        AND w.join_index = m.join_index
        AND w.tx_hash = m.tx_hash
        AND w.evt_index < m.evt_index
    )

    SELECT i.blockchain AS deposit_chain
    , '{{blockchain}}' AS withdrawal_chain
    , 'CCTP' AS bridge_name
    , '1' AS bridge_version
    , w.block_date
    , w.block_time
    , w.block_number
    , w.withdrawal_amount_raw
    , m.sender
    , w.recipient
    , 'erc20' AS withdrawal_token_standard
    , w.withdrawal_token_address
    , w.tx_from
    , w.tx_hash
    , w.evt_index
    , w.contract_address
    , CAST(m.nonce AS varchar) AS bridge_transfer_id
    FROM withdrawals w
    INNER JOIN closest_messages m ON w.block_number = m.block_number
        AND w.join_index = m.join_index
        AND w.tx_hash = m.tx_hash
        AND w.evt_index = m.withdrawal_evt_index
        AND m.rn = 1
    INNER JOIN cctp_id_mapping i ON i.id=m.sourceDomain

{% endmacro %}