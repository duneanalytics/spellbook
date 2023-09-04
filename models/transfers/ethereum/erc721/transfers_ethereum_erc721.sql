{{ config(
    materialized='view',
    alias = alias('erc721'),
    tags= ['dunesql'])
}}

WITH
    received_transfers AS (
        SELECT CONCAT('receive', '-', CAST(evt_tx_hash AS VARCHAR), '-', CAST(evt_index AS VARCHAR), '-', 'to') AS unique_tx_id,
            "to" AS wallet_address,
            contract_address AS token_address,
            evt_block_time,
            tokenId,
            1 AS amount
        FROM
            {{ source('erc721_ethereum', 'evt_transfer') }}
    ),

    sent_transfers AS (
        SELECT CONCAT('send', '-', CAST(evt_tx_hash AS VARCHAR), '-', CAST(evt_index AS varchar), '-', 'from') AS unique_tx_id,
            "from" AS wallet_address,
            contract_address AS token_address,
            evt_block_time,
            tokenId,
            -1 AS amount
        FROM
           {{ source('erc721_ethereum', 'evt_transfer') }}
    )

SELECT 'ethereum' AS blockchain, wallet_address, token_address, evt_block_time, tokenId, amount, unique_tx_id
FROM received_transfers
UNION
SELECT 'ethereum' AS blockchain, wallet_address, token_address, evt_block_time, tokenId, amount, unique_tx_id
FROM sent_transfers