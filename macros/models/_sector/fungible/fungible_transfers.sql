{% macro fungible_transfers(blockchain, native_symbol, traces, transactions, erc20_transfers, erc20_tokens) %}
{%- set token_standard_20 = 'bep20' if blockchain == 'bnb' else 'erc20' -%}
{%- set spark_mode = True -%} {# TODO: Potential bug. Consider disabling #}
{%- set denormalized = True if blockchain in ['base'] else False -%}

WITH transfers AS (
    SELECT block_time
    , block_number
    , value AS amount_raw
    , 0x0000000000000000000000000000000000000000 AS contract_address
    , '{{native_symbol}}' AS symbol
    , 18 AS decimals
    , 'native' AS token_standard
    , "from"
    , to
    , tx_hash
    , NULL AS evt_index
    FROM {{ traces }}
    WHERE success
    AND (call_type NOT IN ('delegatecall', 'callcode', 'staticcall') OR call_type IS NULL)
    AND value > UINT256 '0'
    
    UNION ALL
    
    SELECT t.evt_block_time AS block_time
    , t.evt_block_number AS block_number
    , t.value AS amount_raw
    , t.contract_address
    , tok.symbol
    , tok.decimals
    , '{{token_standard_20}}' AS token_standard
    , t."from"
    , t.to
    , t.evt_tx_hash AS tx_hash
    , t.evt_index
    FROM {{ erc20_transfers }} t
    LEFT JOIN {{ erc20_tokens }} tok ON tok.contract_address=t.contract_address
    )

SELECT '{{blockchain}}' as blockchain
, t.block_time
, t.block_number
, date_trunc('month', t.block_time) AS block_month
, t.amount_raw
, t.amount_raw/t.decimals FILTER (t.decimals IS NOT NULL) AS amount
, pu.price AS usd_price
, CAST(t.amount_raw/t.decimals AS DOUBLE)*pu.price FILTER (t.decimals IS NOT NULL AND pu.price IS NOT NULL) AS usd_amount
, t.contract_address
, t.symbol
--, t.decimals -- afaik no need for decimals since if they're available they should already be applied
, t.token_standard
, t."from"
, t.to
, et."from" AS tx_from
, et."to" AS tx_to
, t.tx_hash
, t.evt_index
FROM transfers t
INNER JOIN {{ transactions }} et ON et.block_number=t.block_number
    AND et.hash=t.tx_hash
INNER JOIN {{ ref('evms_info') }} info ON info.blockchain='{{blockchain}}'
LEFT JOIN {{ ref('prices_usd_forward_fill') }} pu ON pu.blockchain = '{{blockchain}}'
    AND ((pu.contract_address=t.contract_address)
        OR (t.contract_address=0x0000000000000000000000000000000000000000 AND pu.contract_address=info.wrapped_native_token_address)
        )
    AND pu.minute = date_trunc('minute', t.block_time)

{% endmacro %}