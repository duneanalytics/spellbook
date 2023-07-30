{{ config(
        schema='nft_base',
        alias = alias('approvals'),
        tags = ['dunesql'],
        partition_by=['block_date'],
        materialized='incremental',
        incremental_strategy='merge',
        file_format = 'delta',
        unique_key = ['block_number','tx_hash','evt_index']
)
}}

SELECT 'base' AS blockchain
, app.evt_block_time AS block_time
, cast( date_trunc('day', app.evt_block_time) as date) AS block_date
, app.evt_block_number AS block_number
, app.owner AS address
, 'erc721' AS token_standard
, CAST(false AS boolean) AS approval_for_all
, app.contract_address
, CAST(app.tokenId AS double) AS token_id
, CASE WHEN approved = 0x0000000000000000000000000000000000000000 then false else true end as approved
, app.evt_tx_hash AS tx_hash
--, et.from AS tx_from
--, et.to AS tx_to
, app.evt_index
FROM {{ source('erc721_base','evt_Approval') }} app
/*INNER JOIN {{ source('base', 'transactions') }} et ON et.block_number=app.evt_block_number
    AND et.hash=app.evt_tx_hash
    {% if is_incremental() %}
    AND et.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}*/
{% if is_incremental() %}
WHERE app.evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}

UNION ALL

SELECT 'base' AS blockchain
, app.evt_block_time AS block_time
, cast( date_trunc('day', app.evt_block_time) as date) AS block_date
, app.evt_block_number AS block_number
, app.owner AS address
, 'erc721' AS token_standard
, CAST(true AS boolean) AS approval_for_all
, app.contract_address
, CAST(NULL AS double) AS token_id
, approved
, app.evt_tx_hash AS tx_hash
--, et.from AS tx_from
--, et.to AS tx_to
, app.evt_index
FROM {{ source('erc721_base','evt_ApprovalForAll') }} app
/*INNER JOIN {{ source('base', 'transactions') }} et ON et.block_number=app.evt_block_number
    AND et.hash=app.evt_tx_hash
    {% if is_incremental() %}
    AND et.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}*/
{% if is_incremental() %}
WHERE app.evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}

UNION ALL

SELECT 'base' AS blockchain
, app.evt_block_time AS block_time
, cast( date_trunc('day', app.evt_block_time) as date) AS block_date
, app.evt_block_number AS block_number
, app.operator AS address
, 'erc1155' AS token_standard
, CAST(true AS boolean) AS approval_for_all
, app.contract_address
, CAST(NULL AS double) AS token_id
, approved
, app.evt_tx_hash AS tx_hash
--, et.from AS tx_from
--, et.to AS tx_to
, app.evt_index
FROM {{ source('erc1155_base','evt_ApprovalForAll') }} app
/*INNER JOIN {{ source('base', 'transactions') }} et ON et.block_number=app.evt_block_number
    AND et.hash=app.evt_tx_hash
    {% if is_incremental() %}
    AND et.block_time >= date_trunc('day', now() - interval '7' day)
    {% endif %}*/
{% if is_incremental() %}
WHERE app.evt_block_time >= date_trunc('day', now() - interval '7' day)
{% endif %}