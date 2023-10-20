{% macro transfers_base(blockchain, traces, transactions, erc20_transfers, wrapped_token_deposit = null, wrapped_token_withdrawal = null) %}
{%- set token_standard_20 = 'bep20' if blockchain == 'bnb' else 'erc20' -%}
{# denormalized tables are not yet in use #}
{%- set denormalized = True if blockchain in ['base'] else False -%}

WITH transfers AS (
    SELECT block_time
    , block_number
    , tx_hash
    , value AS amount_raw
    , CAST(NULL AS varbinary) AS contract_address
    , 'native' AS token_standard
    , "from"
    , to
    , NULL AS evt_index
    , trace_address
    FROM {{ traces }}
    WHERE success
    AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS null)
    AND value > UINT256 '0'
    {% if is_incremental() %}
    AND {{incremental_predicate('block_time')}}
    {% endif %}

    UNION ALL

    SELECT t.evt_block_time AS block_time
    , t.evt_block_number AS block_number
    , t.evt_tx_hash AS tx_hash
    , t.value AS amount_raw
    , t.contract_address
    , '{{token_standard_20}}' AS token_standard
    , t."from"
    , t.to
    , t.evt_index
    , NULL AS trace_address
    FROM {{ erc20_transfers }} t
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% endif %}

    {% if wrapped_token_deposit and wrapped_token_withdrawal %}
    UNION ALL

    SELECT t.evt_block_time AS block_time
    , t.evt_block_number AS block_number
    , t.evt_tx_hash AS tx_hash
    , t.value AS amount_raw
    , t.contract_address
    -- technically this is not a standard 20 token, but we use it for consistency
    , '{{token_standard_20}}' AS token_standard
    , t."from"
    , t.to
    , t.evt_index
    , NULL AS trace_address
    FROM {{ wrapped_token_deposit }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% endif %}

    UNION ALL

    SELECT t.evt_block_time AS block_time
    , t.evt_block_number AS block_number
    , t.evt_tx_hash AS tx_hash
    , t.value AS amount_raw
    , t.contract_address
    -- technically this is not a standard 20 token, but we use it for consistency
    , '{{token_standard_20}}' AS token_standard
    , t."from"
    , t.to
    , t.evt_index
    , NULL AS trace_address
    FROM {{ wrapped_token_withdrawal }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% endif %}
{% endif %}
    )

SELECT '{{blockchain}}' as blockchain
, cast(date_trunc('day', t.block_time) as date) as block_date
, t.block_time
, t.block_number
, t.tx_hash
-- method_id => first 4bytes of data
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