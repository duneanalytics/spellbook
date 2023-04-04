{{ config(
        alias ='approvals',
        partition_by='block_date',
        materialized='incremental',
        file_format = 'delta',
        unique_key = ['unique_approval_id']
)
}}

SELECT 'bnb' AS blockchain
, app.evt_block_time AS block_time
, date_trunc('day', app.evt_block_time) AS block_date
, app.evt_block_number AS block_number
, app.owner AS address
, 'erc721' AS token_standard
, CAST(false AS boolean) AS approval_for_all
, app.contract_address
, CAST(app.tokenId AS DECIMAL(38,0)) AS token_id
, approved 
, CAST(NULL AS boolean) AS approved_for_all
, app.evt_tx_hash AS tx_hash
, et.from AS tx_from
, et.to AS tx_to
, app.evt_index
, 'bnb' || '-' || app.evt_tx_hash || '-' || app.evt_index AS unique_approval_id
FROM {{ source('erc721_bnb','evt_Approval') }} app
INNER JOIN {{ source('bnb', 'transactions') }} et ON et.block_number=app.evt_block_number
    AND et.hash=app.evt_tx_hash
    {% if is_incremental() %}
    AND et.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
{% if is_incremental() %}
WHERE app.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}

UNION ALL

SELECT 'bnb' AS blockchain
, app.evt_block_time AS block_time
, date_trunc('day', app.evt_block_time) AS block_date
, app.evt_block_number AS block_number
, app.owner AS address
, 'erc721' AS token_standard
, CAST(true AS boolean) AS approval_for_all
, app.contract_address
, CAST(NULL AS DECIMAL(38,0)) AS token_id
, NULL AS approved 
, CAST(approved AS boolean) AS approved_for_all
, app.evt_tx_hash AS tx_hash
, et.from AS tx_from
, et.to AS tx_to
, app.evt_index
, 'bnb' || '-' || app.evt_tx_hash || '-' || app.evt_index AS unique_approval_id
FROM {{ source('erc721_bnb','evt_ApprovalForAll') }} app
INNER JOIN {{ source('bnb', 'transactions') }} et ON et.block_number=app.evt_block_number
    AND et.hash=app.evt_tx_hash
    {% if is_incremental() %}
    AND et.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
{% if is_incremental() %}
WHERE app.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}

UNION ALL

SELECT 'bnb' AS blockchain
, app.evt_block_time AS block_time
, date_trunc('day', app.evt_block_time) AS block_date
, app.evt_block_number AS block_number
, app.operator AS address
, 'erc1155' AS token_standard
, CAST(true AS boolean) AS approval_for_all
, app.contract_address
, CAST(NULL AS DECIMAL(38,0)) AS token_id
, NULL AS approved 
, CAST(approved AS boolean) AS approved_for_all
, app.evt_tx_hash AS tx_hash
, et.from AS tx_from
, et.to AS tx_to
, app.evt_index
, 'bnb' || '-' || app.evt_tx_hash || '-' || app.evt_index AS unique_approval_id
FROM {{ source('erc1155_bnb','evt_ApprovalForAll') }} app
INNER JOIN {{ source('bnb', 'transactions') }} et ON et.block_number=app.evt_block_number
    AND et.hash=app.evt_tx_hash
    {% if is_incremental() %}
    AND et.block_time >= date_trunc("day", now() - interval '1 week')
    {% endif %}
{% if is_incremental() %}
WHERE app.evt_block_time >= date_trunc("day", now() - interval '1 week')
{% endif %}