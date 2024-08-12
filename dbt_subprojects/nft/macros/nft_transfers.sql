{% macro nft_transfers(blockchain, base_transactions, erc721_transfers, erc1155_single, erc1155_batch ) %}
{%- set token_standard_721 = 'bep721' if blockchain == 'bnb' else 'erc721' -%}
{%- set token_standard_1155 = 'bep1155' if blockchain == 'bnb' else 'erc1155' -%}
{%- set spark_mode = True -%} {# TODO: Potential bug. Consider disabling #}
{%- set denormalized = True if blockchain in ['base'] else False -%}
SELECT
    *
FROM(
     SELECT '{{blockchain}}' as blockchain
    , t.evt_block_time AS block_time
    , cast(date_trunc('month', t.evt_block_time) as date) AS block_month
    , cast(date_trunc('day', t.evt_block_time) as date) AS block_date
    , t.evt_block_number AS block_number
    , '{{token_standard_721}}' AS token_standard
    , 'single' AS transfer_type
    , t.evt_index
    , t.contract_address
    , t.tokenId AS token_id
    , cast(1 as uint256) AS amount
    , t."from"
    , t.to
    {% if denormalized == True -%}
    , t.evt_tx_from AS executed_by
    {%- else -%}
    , et."from" AS executed_by
    {%- endif %}
    , t.evt_tx_hash AS tx_hash
    , {{ dbt_utils.generate_surrogate_key(['t.evt_tx_hash', 't.evt_index', 't.tokenId', 1]) }} as unique_transfer_id -- For backward compatibility
    FROM {{ erc721_transfers }} t
    {% if denormalized == False %}
    INNER JOIN {{ base_transactions }} et ON et.block_number = t.evt_block_number
        AND et.hash = t.evt_tx_hash
        {% if is_incremental() %}
        AND {{incremental_predicate('et.block_time')}}
        {% endif %}
    {%- endif -%}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('t.evt_block_time')}}
    {% endif %}

    UNION ALL

    SELECT '{{blockchain}}' as blockchain
    , t.evt_block_time AS block_time
    , cast(date_trunc('month', t.evt_block_time) as date) AS block_month
    , cast(date_trunc('day', t.evt_block_time) as date) AS block_date
    , t.evt_block_number AS block_number
    , '{{token_standard_1155}}' AS token_standard
    , 'single' AS transfer_type
    , t.evt_index
    , t.contract_address
    , t.id AS token_id
    , t.value AS amount
    , t."from"
    , t.to
    {% if denormalized == True -%}
    , t.evt_tx_from AS executed_by
    {%- else -%}
    , et."from" AS executed_by
    {%- endif %}
    , t.evt_tx_hash AS tx_hash
    , {{ dbt_utils.generate_surrogate_key(['t.evt_tx_hash', 't.evt_index', 't.id', 't.value']) }} as unique_transfer_id -- For backward compatibility
    FROM {{ erc1155_single }} t
    {%- if denormalized == False %}
    INNER JOIN {{ base_transactions }} et ON et.block_number = t.evt_block_number
        AND et.hash = t.evt_tx_hash
        {% if is_incremental() %}
        AND {{incremental_predicate('et.block_time')}}
        {% endif %}
    {%- endif -%}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('t.evt_block_time')}}
    {% endif %}

    UNION ALL

    SELECT '{{blockchain}}' as blockchain
    , t.evt_block_time AS block_time
    , cast(date_trunc('month', t.evt_block_time) as date) AS block_month
    , cast(date_trunc('day', t.evt_block_time) as date) AS block_date
    , t.evt_block_number AS block_number
    , '{{token_standard_1155}}'  AS token_standard
    , 'batch' AS transfer_type
    , t.evt_index
    , t.contract_address
    , t.id AS token_id
    , cast(t.value as uint256) AS amount
    , t."from"
    , t.to
    {% if denormalized == True -%}
    , t.evt_tx_from AS executed_by
    {%- else -%}
    , et."from" AS executed_by
    {%- endif %}
    , t.evt_tx_hash AS tx_hash
    , {{ dbt_utils.generate_surrogate_key(['t.evt_tx_hash', 't.evt_index', 't.id', 't.value']) }} as unique_transfer_id -- For backward compatibility
    FROM (
        SELECT t.evt_block_time, t.evt_block_number, t.evt_tx_hash, t.contract_address, t."from", t.to, t.evt_index {% if denormalized == True %}, t.evt_tx_from {% endif %}
        , value, id
        FROM {{ erc1155_batch }} t
        CROSS JOIN unnest(zip(t."values", t.ids)) AS foo(value, id)
        {% if is_incremental() %}
        WHERE {{incremental_predicate('t.evt_block_time')}}
        {% endif %}
        {% if spark_mode == True %}
        {# This deduplicates rows. Double check if this is correct or not #}
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9 {% if denormalized == True %}, 10 {% endif %}
        {% endif %}
        ) t
    {%- if denormalized == False %}
    INNER JOIN {{ base_transactions }} et ON et.block_number = t.evt_block_number
        AND et.hash = t.evt_tx_hash
        {% if is_incremental() %}
        AND {{incremental_predicate('et.block_time')}}
        {% endif %}
    {%- endif -%}
    {% if spark_mode == True %}
    {# TODO: This is a bug. In the comparsion t.value > 0, spark converts t.value to an integer before the comparison,
    or null (i.e., false) if it overflows) #}
    WHERE t.value > uint256 '0' and t.value < uint256 '{{2**31}}'
    {% else %}
    WHERE t.value > uint256 '0'
    {% endif %}
)

{% endmacro %}
