{% macro transfers_base_wrapped_token(blockchain, transactions, wrapped_token_deposit, wrapped_token_withdrawal) %}
{%- set token_standard_20 = 'bep20' if blockchain == 'bnb' else 'erc20' -%}
{%- set default_address = '0x0000000000000000000000000000000000000000' -%}

with transfers AS (
    SELECT
        t.evt_block_time AS block_time
        , t.evt_block_number AS block_number
        , t.evt_tx_hash AS tx_hash
        , t.evt_index
        , CAST(NULL AS ARRAY<BIGINT>) AS trace_address
        , t.contract_address
        , '{{token_standard_20}}' AS token_standard -- technically this is not a standard 20 token, but we use it for consistency
        , {{default_address}} AS "from"
        , t.dst as "to"
        , t.wad AS amount_raw -- is this safe cross chain?
    FROM {{ wrapped_token_deposit }} t
    {% if is_incremental() %}
    WHERE {{incremental_predicate('t.evt_block_time')}}
    {% endif %}

    UNION ALL

    SELECT
        t.evt_block_time AS block_time
        , t.evt_block_number AS block_number
        , t.evt_tx_hash AS tx_hash
        , t.evt_index
        , CAST(NULL AS ARRAY<BIGINT>) AS trace_address
        , t.contract_address
        , '{{token_standard_20}}' AS token_standard -- technically this is not a standard 20 token, but we use it for consistency
        , t.src as "from"
        , {{default_address}} AS "to"
        , t.wad AS amount_raw -- is this safe cross chain?
    FROM {{ wrapped_token_withdrawal }} t
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% endif %}
    )

SELECT
    -- We have to create this unique key because evt_index and trace_address can be null
    {{dbt_utils.generate_surrogate_key(['t.block_number', 'tx.index', 't.evt_index', "array_join(trace_address, ',')"])}} as unique_key
    , '{{blockchain}}' as blockchain
    , cast(date_trunc('day', t.block_time) as date) as block_date
    , t.block_time
    , t.block_number
    , t.tx_hash
    , t.evt_index
    , t.trace_address
    , t.token_standard
    , tx."from" AS tx_from
    , tx."to" AS tx_to
    , tx."index" AS tx_index
    , t."from"
    , t.to
    , t.contract_address
    , t.amount_raw
FROM transfers t
INNER JOIN {{ transactions }} tx ON
    tx.block_number = t.block_number
    AND tx.hash = t.tx_hash
    {% if is_incremental() %}
    AND {{incremental_predicate('tx.block_time')}}
    {% endif %}
{% endmacro %}