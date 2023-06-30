{{ config(
        alias = alias('transfers'),
        tags = ['dunesql'],
        partition_by='block_date',
        materialized='incremental',
        file_format = 'delta',
        unique_key = ['unique_transfer_id']
)
}}

 SELECT 'optimism' as blockchain
, t.evt_block_time AS block_time
, date_trunc('day', t.evt_block_time) AS block_date
, t.evt_block_number AS block_number
, 'erc721' AS token_standard
, 'single' AS transfer_type
, t.evt_index
, t.contract_address
, t.tokenId AS token_id
, cast(1 as uint256) AS amount
, t."from"
, t.to
, ot."from" AS executed_by
, t.evt_tx_hash AS tx_hash
, 'optimism' || cast(t.evt_tx_hash as varchar) || '-erc721-' || cast(t.contract_address as varchar) || '-' || cast(t.tokenId as varchar) || '-' || cast(t."from" as varchar) || '-' || cast(t.to as varchar) || '-' || '1' || '-' || cast(t.evt_index as varchar) AS unique_transfer_id
FROM {{ source('erc721_optimism','evt_transfer') }} t
{% if is_incremental() %}
    ANTI JOIN {{this}} anti_table
        ON t.evt_tx_hash = anti_table.tx_hash
{% endif %}
INNER JOIN {{ source('optimism', 'transactions') }} ot ON ot.block_number = t.evt_block_number
    AND ot.hash = t.evt_tx_hash
    {% if is_incremental() %}
    AND ot.block_time >= date_trunc("day", now() - interval '7' day)
    {% endif %}
{% if is_incremental() %}
WHERE t.evt_block_time >= date_trunc("day", now() - interval '7' day)
{% endif %}

UNION ALL

SELECT 'optimism' as blockchain
, t.evt_block_time AS block_time
, date_trunc('day', t.evt_block_time) AS block_date
, t.evt_block_number AS block_number
, 'erc1155' AS token_standard
, 'single' AS transfer_type
, t.evt_index
, t.contract_address
, t.id AS token_id
, t.value AS amount
, t."from"
, t.to
, ot."from" AS executed_by
, t.evt_tx_hash AS tx_hash
, 'optimism' || cast(t.evt_tx_hash as varchar) || '-erc1155-' || cast(t.contract_address as varchar) || '-' || cast(t.id as varchar) || '-' || cast(t."from" as varchar) || '-' || cast(t.to as varchar) || '-' || cast(t.value as varchar) || '-' || cast(t.evt_index as varchar) AS unique_transfer_id
FROM {{ source('erc1155_optimism','evt_transfersingle') }} t
{% if is_incremental() %}
    ANTI JOIN {{this}} anti_table
        ON t.evt_tx_hash = anti_table.tx_hash
{% endif %}
INNER JOIN {{ source('optimism', 'transactions') }} ot ON ot.block_number = t.evt_block_number
    AND ot.hash = t.evt_tx_hash
    {% if is_incremental() %}
    AND ot.block_time >= date_trunc("day", now() - interval '7' day)
    {% endif %}
{% if is_incremental() %}
WHERE t.evt_block_time >= date_trunc("day", now() - interval '7' day)
{% endif %}

UNION ALL

SELECT 'optimism' as blockchain
, t.evt_block_time AS block_time
, date_trunc('day', t.evt_block_time) AS block_date
, t.evt_block_number AS block_number
, 'erc1155' AS token_standard
, 'batch' AS transfer_type
, t.evt_index
, t.contract_address
, t.token_id
, t.amount
, t."from"
, t.to
, ot."from" AS executed_by
, evt_tx_hash AS tx_hash
, 'optimism' || cast(t.evt_tx_hash as varchar) || '-erc1155-' || cast(t.contract_address as varchar) || '-' || cast(token_id as varchar) || '-' || cast(t."from" as varchar) || '-' || cast(t.to as varchar) || '-' || cast(t.amount as varchar) || '-' || cast(t.evt_index as varchar) AS unique_transfer_id
FROM (
    SELECT t.evt_block_time, t.evt_block_number, t.evt_tx_hash, t.contract_address, t."from", t.to, t.evt_index
    , u.token_id, u.amount

    FROM {{ source('erc1155_optimism', 'evt_transferbatch') }} t
    CROSS JOIN unnest(
        transform(
        sequence(1, least(cardinality(t."values"), cardinality(t.ids))),
            i -> cast(row(t."values"[i], t.ids[i]) AS ROW(value uint256 , id uint256))
        ) 
    ) 
    AS u(amount , token_id )

    {% if is_incremental() %}
        ANTI JOIN {{this}} anti_table
            ON t.evt_tx_hash = anti_table.tx_hash
    {% endif %}
    {% if is_incremental() %}
    WHERE t.evt_block_time >= date_trunc("day", now() - interval '7' day)
    {% endif %}
    GROUP BY t.evt_block_time, t.evt_block_number, t.evt_tx_hash, t.contract_address, t."from", t.to, t.evt_index, u.token_id, u.amount
    ) t
INNER JOIN {{ source('optimism', 'transactions') }} ot ON ot.block_number = t.evt_block_number
    AND ot.hash = t.evt_tx_hash
    {% if is_incremental() %}
    AND ot.block_time >= date_trunc("day", now() - interval '7' day)
    {% endif %}
WHERE amount > cast(0 as uint256)
GROUP BY t.evt_block_time, t.evt_block_number, t.evt_tx_hash, t.contract_address, t."from", t.to, ot."from", t.evt_index, token_id, amount
