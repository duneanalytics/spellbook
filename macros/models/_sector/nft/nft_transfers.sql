{% macro nft_transfers(blockchain, base_transactions, erc721_transfers, erc1155_single, erc1155_batch ) %}
{%- set token_standard_721 = 'bep721' if blockchain == 'bnb' else 'erc721' -%}
{%- set token_standard_1155 = 'bep1155' if blockchain == 'bnb' else 'erc1155' -%}
{%- set spark_mode = True -%} {# TODO: Potential bug. Consider disabling #}
SELECT
    *
FROM(
     SELECT '{{blockchain}}' as blockchain
    , t.evt_block_time AS block_time
    , date_trunc('day', t.evt_block_time) AS block_date
    , t.evt_block_number AS block_number
    , '{{token_standard_721}}' AS token_standard
    , 'single' AS transfer_type
    , t.evt_index
    , t.contract_address
    , t.tokenId AS token_id
    , cast(1 as uint256) AS amount
    , t."from"
    , t.to
    , et."from" AS executed_by
    , t.evt_tx_hash AS tx_hash
    , '{{blockchain}}' || cast(t.evt_tx_hash as varchar) || '-{{token_standard_721}}-' || cast(t.contract_address as varchar) || '-' || cast(t.tokenId as varchar) || '-' || cast(t."from" as varchar) || '-' || cast(t.to as varchar) || '-1-' || cast(t.evt_index as varchar) AS unique_transfer_id
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
    , '{{token_standard_1155}}' AS token_standard
    , 'single' AS transfer_type
    , t.evt_index
    , t.contract_address
    , t.id AS token_id
    , t.value AS amount
    , t."from"
    , t.to
    , et."from" AS executed_by
    , t.evt_tx_hash AS tx_hash
    , '{{blockchain}}' || cast(t.evt_tx_hash as varchar) || '-{{token_standard_1155}}-' || cast(t.contract_address as varchar) || '-' || cast(t.id as varchar) || '-' || cast(t."from" as varchar) || '-' || cast(t.to as varchar) || '-' || cast(t.value as varchar) || '-' || cast(t.evt_index as varchar) AS unique_transfer_id
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
    , '{{token_standard_1155}}'  AS token_standard
    , 'batch' AS transfer_type
    , t.evt_index
    , t.contract_address
    , t.id AS token_id
    , cast(t.value as uint256) AS amount
    , t."from"
    , t.to
    , et."from" AS executed_by
    , t.evt_tx_hash AS tx_hash
    , '{{blockchain}}' || cast(t.evt_tx_hash as varchar) || '-{{token_standard_1155}}-' || cast(t.contract_address as varchar) || '-' || cast(t.id as varchar) || '-' || cast(t."from" as varchar) || '-' || cast(t.to as varchar) || '-' || cast(t.value as varchar) || '-' || cast(t.evt_index as varchar) AS unique_transfer_id
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
        {% if spark_mode == True %}
        {# This deduplicates rows. Double check if this is correct or not #}
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9
        {% endif %}
        ) t
    INNER JOIN {{ base_transactions }} et ON et.block_number = t.evt_block_number
        AND et.hash = t.evt_tx_hash
        {% if is_incremental() %}
        AND et.block_time >= date_trunc('day', now() - interval '7' day)
        {% endif %}
    {% if spark_mode == True %}
    {# TODO: This is a bug. In the comparsion t.value > 0, spark converts t.value to an integer before the comparison,
    or null (i.e., false) if it overflows) #}
    WHERE t.value > uint256 '0' and t.value < uint256 '{{2**31}}'
    {% else %}
    WHERE t.value > uint256 '0'
    {% endif %}
)

{% endmacro %}
