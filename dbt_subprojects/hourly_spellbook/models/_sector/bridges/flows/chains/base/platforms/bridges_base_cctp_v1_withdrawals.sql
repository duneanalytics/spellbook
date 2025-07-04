{% set blockchain = 'base' %}

{{ config(
    schema = 'bridges_' + blockchain,
    alias = 'cctp_v1_withdrawals',
    materialized = 'view',
    )
}}

WITH cctp_id_mapping AS (
    SELECT id, blockchain, token_standard
    FROM (VALUES
    (0, 'ethereum', 'erc20')
    , (1, 'avalanche_c', 'erc20')
    , (2, 'optimism', 'erc20')
    , (3, 'arbitrum', 'erc20')
    , (4, 'noble', 'ics20')
    , (5, 'solana', 'spl')
    , (6, 'base', 'erc20')
    , (7, 'polygon', 'erc20')
    , (8, 'sui', 'move module')
    , (9, 'aptos', 'move module')
    , (10, 'unichain', 'erc20')
    , (11, 'linea', 'erc20')
    , (12, 'codex', 'erc20')
    , (13, 'sonic', 'erc20')
    , (14, 'worldchain', 'erc20')
    ) AS x (id, blockchain, token_standard)
    )

, messages AS (
    SELECT evt_block_number AS block_number
    , evt_tx_hash AS tx_hash
    , CASE WHEN varbinary_substring(sender,1, 12) = 0x000000000000000000000000 THEN varbinary_substring(sender,13) ELSE sender END AS sender
    , nonce
    , evt_index
    , sourceDomain
    , ROW_NUMBER() OVER (PARTITION BY evt_block_number, evt_tx_hash ORDER BY evt_index) AS join_index
    FROM {{ source('circle_base', 'messagetransmitter_evt_messagereceived')}}
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
    FROM {{ source('circle_base', 'tokenmessenger_evt_mintandwithdraw')}} w
    )

    SELECT i.blockchain AS deposit_chain
    , 'base' AS withdrawal_chain
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
    , CAST(m.nonce AS varchar) AS transfer_id
    FROM withdrawals w
    INNER JOIN messages m ON w.block_number = m.block_number
        and w.join_index = m.join_index
    INNER JOIN cctp_id_mapping i ON i.id=m.sourceDomain