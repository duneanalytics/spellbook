{% macro transfers_base_native(blockchain, traces, transactions, native_contract_address = null) %}

WITH transfers AS (
    SELECT
        block_time,
        , block_date
        , block_number
        , tx_index
        , tx_hash
        , cast(NULL as bigint) AS evt_index
        , trace_address
        {% if native_contract_address%}
        , {{native_contract_address}} AS contract_address
        {% else %}
        , CAST(NULL AS varbinary) AS contract_address
        {% endif %}
        , 'native' AS token_standard
        , "from"
        , to
        , value AS amount_raw
    FROM {{ traces }}
    WHERE success
        AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null)
        AND value > UINT256 '0'
        {% if is_incremental() %}
        AND {{incremental_predicate('block_time')}}
        {% endif %}
)

SELECT
    -- We have to create this unique key because evt_index and trace_address can be null
    {{dbt_utils.generate_surrogate_key(['t.block_number', 'tx.index', 't.evt_index', "array_join(t.trace_address, ',')"])}} as unique_key
    , '{{blockchain}}' as blockchain
    ,  block_date
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
    tx.block_date = t.block_date
    AND tx.block_number = t.block_number
    AND tx.index = t.tx_index
    {% if is_incremental() %}
    AND {{incremental_predicate('tx.block_time')}}
    {% endif %}
{% endmacro %}