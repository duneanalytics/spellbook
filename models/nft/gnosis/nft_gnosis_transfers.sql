{{ config(
        alias ='transfers',
        partition_by='block_date',
        materialized='incremental',
        file_format = 'delta',
        post_hook='{{ expose_spells(\'["gnosis"]\',
                                    "sector",
                                    "nft",
                                    \'["hildobby"]\') }}',
        unique_key = ['unique_transfer_id']
)
}}

 SELECT evt_block_time AS block_time
, date_trunc('day', evt_block_time) AS block_date
, evt_block_number AS block_number
, 'erc721' AS token_standard
, 'single' AS transfer_type
, evt_index
, contract_address
, tokenId AS token_id
, 1 AS amount
, from
, to
, evt_tx_hash AS tx_hash
, 'gnosis' || evt_tx_hash || '-erc721-' || contract_address || '-' || tokenId || '-' || from || '-' || to || '-' || '1' || '-' || evt_index AS unique_transfer_id
FROM {{ source('erc721_gnosis','evt_transfer') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}
UNION
SELECT evt_block_time AS block_time
, date_trunc('day', evt_block_time) AS block_date
, evt_block_number AS block_number
, 'erc1155' AS token_standard
, 'single' AS transfer_type
, evt_index
, contract_address
, id AS token_id
, value AS amount
, from
, to
, evt_tx_hash AS tx_hash
, 'gnosis' || evt_tx_hash || '-erc721-' || contract_address || '-' || id || '-' || from || '-' || to || '-' || value || '-' || evt_index AS unique_transfer_id
FROM {{ source('erc1155_gnosis','evt_transfersingle') }}
{% if is_incremental() %}
WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}
UNION
SELECT evt_block_time AS block_time
, date_trunc('day', evt_block_time) AS block_date
, evt_block_number AS block_number
, 'erc1155' AS token_standard
, 'batch' AS transfer_type
, evt_index
, contract_address
, ids_and_count.ids AS token_id
, ids_and_count.values AS amount
, from
, to
, evt_tx_hash AS tx_hash
, 'gnosis' || evt_tx_hash || '-erc1155-' || contract_address || '-' || ids_and_count.ids || '-' || from || '-' || to || '-' || ids_and_count.values || '-' || evt_index AS unique_transfer_id
FROM (
    SELECT evt_block_time, evt_block_number, evt_tx_hash, contract_address, from, to, evt_index
    , explode(arrays_zip(values, ids)) AS ids_and_count
    FROM {{ source('erc1155_gnosis', 'evt_transferbatch') }}
    {% if is_incremental() %}
    WHERE evt_block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
    GROUP BY evt_block_time, evt_block_number, evt_tx_hash, contract_address, from, to, evt_index, values, ids
    )
WHERE ids_and_count.values > 0
GROUP BY evt_block_time, evt_block_number, evt_tx_hash, contract_address, from, to, evt_index, token_id, amount