{% macro nft_transfers(blockchain, base_transactions, erc721_transfers, erc1155_single, erc1155_batch ) %}
SELECT
    *
    , blockchain ||'-'|| cast(block_number as varchar) ||'-'|| cast(tx_hash as varchar) ||'-'|| cast(evt_index as varchar) AS unique_transfer_id
FROM(
     SELECT '{{blockchain}}' as blockchain
    , t.evt_block_time AS block_time
    , date_trunc('day', t.evt_block_time) AS block_date
    , t.evt_block_number AS block_number
    , case when '{{blockchain}}' = 'bnb' then 'bep721' else 'erc721' end AS token_standard
    , 'single' AS transfer_type
    , t.evt_index
    , t.contract_address
    , t.tokenId AS token_id
    , cast(1 as uint256) AS amount
    , t."from"
    , t.to
    , et."from" AS executed_by
    , t.evt_tx_hash AS tx_hash
    FROM {{ erc721_transfers }} t
        {% if is_incremental() %}
            LEFT JOIN {{this}} anti_table
                ON t.evt_tx_hash = anti_table.tx_hash
        {% endif %}
    INNER JOIN {{ base_transactions }} et ON et.block_number = t.evt_block_number
        AND et.hash = t.evt_tx_hash
        {% if is_incremental() %}
        AND et.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    {% if is_incremental() %}
    WHERE t.evt_block_time >= date_trunc('day', now() - interval '7' day)
    AND anti_table.tx_hash is null
    {% endif %}

    UNION ALL

    SELECT '{{blockchain}}' as blockchain
    , t.evt_block_time AS block_time
    , date_trunc('day', t.evt_block_time) AS block_date
    , t.evt_block_number AS block_number
    , case when '{{blockchain}}' = 'bnb' then 'bep1155' else 'erc1155' end AS token_standard
    , 'single' AS transfer_type
    , t.evt_index
    , t.contract_address
    , t.id AS token_id
    , t.value AS amount
    , t."from"
    , t.to
    , et."from" AS executed_by
    , t.evt_tx_hash AS tx_hash
    FROM {{ erc1155_single }} t
        {% if is_incremental() %}
            LEFT JOIN {{this}} anti_table
                ON t.evt_tx_hash = anti_table.tx_hash
        {% endif %}
    INNER JOIN {{ base_transactions }} et ON et.block_number = t.evt_block_number
        AND et.hash = t.evt_tx_hash
        {% if is_incremental() %}
        AND et.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    {% if is_incremental() %}
    WHERE t.evt_block_time >= date_trunc('day', now() - interval '7' day)
    AND anti_table.tx_hash is null
    {% endif %}

    UNION ALL

    SELECT '{{blockchain}}' as blockchain
    , t.evt_block_time AS block_time
    , date_trunc('day', t.evt_block_time) AS block_date
    , t.evt_block_number AS block_number
    , case when '{{blockchain}}' = 'bnb' then 'bep1155' else 'erc1155' end AS token_standard
    , 'batch' AS transfer_type
    , t.evt_index
    , t.contract_address
    , t.id AS token_id
    , cast(t.value as uint256) AS amount
    , t."from"
    , t.to
    , et."from" AS executed_by
    , t.evt_tx_hash AS tx_hash
    FROM (
        SELECT t.evt_block_time, t.evt_block_number, t.evt_tx_hash, t.contract_address, t."from", t.to, t.evt_index
        , value, id
        FROM {{ erc1155_batch }} t
        CROSS JOIN unnest(zip(t."values", t.ids)) AS foo(value, id)
        {% if is_incremental() %}
        LEFT JOIN {{this}} anti_table
            ON t.evt_tx_hash = anti_table.tx_hash
        WHERE t.evt_block_time >= date_trunc('day', now() - interval '7' day)
            AND anti_table.tx_hash is null
        {% endif %}
        ) t
    INNER JOIN {{ base_transactions }} et ON et.block_number = t.evt_block_number
        AND et.hash = t.evt_tx_hash
        {% if is_incremental() %}
        AND et.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    WHERE t.value > cast(0 as uint256)
)

{% endmacro %}
