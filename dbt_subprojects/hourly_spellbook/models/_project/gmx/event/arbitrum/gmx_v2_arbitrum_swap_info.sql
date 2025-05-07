{{
  config(
    schema = 'gmx_v2_arbitrum',
    alias = 'swap_info',
    materialized = 'incremental',
    unique_key = ['tx_hash', 'index'],
    incremental_strategy = 'merge'
    )
}}

{%- set event_name = 'SwapInfo' -%}
{%- set blockchain_name = 'arbitrum' -%}

WITH evt_data_1 AS (
    SELECT 
        -- Main Variables
        '{{ blockchain_name }}' AS blockchain,
        evt_block_time AS block_time,
        evt_block_number AS block_number, 
        evt_tx_hash AS tx_hash,
        evt_index AS index,
        contract_address,
        eventName AS event_name,
        eventData AS data,
        msgSender AS msg_sender
    FROM {{ source('gmx_v2_arbitrum','EventEmitter_evt_EventLog1')}}
    WHERE eventName = '{{ event_name }}'
    {% if is_incremental() %}
        AND {{ incremental_predicate('evt_block_time') }}
    {% endif %}
)

, evt_data_2 AS (
    SELECT 
        -- Main Variables
        '{{ blockchain_name }}' AS blockchain,
        evt_block_time AS block_time,
        evt_block_number AS block_number, 
        evt_tx_hash AS tx_hash,
        evt_index AS index,
        contract_address,
        eventName AS event_name,
        eventData AS data,
        msgSender AS msg_sender
    FROM {{ source('gmx_v2_arbitrum','EventEmitter_evt_EventLog2')}}
    WHERE eventName = '{{ event_name }}'
    {% if is_incremental() %}
        AND {{ incremental_predicate('evt_block_time') }}
    {% endif %}
)

-- unite 2 tables
, evt_data AS (
    SELECT * 
    FROM evt_data_1
    UNION ALL
    SELECT *
    FROM evt_data_2
)

, parsed_data AS (
    SELECT
        tx_hash,
        index, 
        json_query(data, 'lax $.addressItems' OMIT QUOTES) AS address_items,
        json_query(data, 'lax $.uintItems' OMIT QUOTES) AS uint_items,
        json_query(data, 'lax $.intItems' OMIT QUOTES) AS int_items,
        json_query(data, 'lax $.bytes32Items' OMIT QUOTES) AS bytes32_items
    FROM
        evt_data
)

, address_items_parsed AS (
    SELECT 
        tx_hash,
        index,
        json_extract_scalar(CAST(item AS VARCHAR), '$.key') AS key_name,
        json_extract_scalar(CAST(item AS VARCHAR), '$.value') AS value
    FROM 
        parsed_data,
        UNNEST(
            CAST(json_extract(address_items, '$.items') AS ARRAY(JSON))
        ) AS t(item)
)

, uint_items_parsed AS (
    SELECT 
        tx_hash,
        index,
        json_extract_scalar(CAST(item AS VARCHAR), '$.key') AS key_name,
        json_extract_scalar(CAST(item AS VARCHAR), '$.value') AS value
    FROM 
        parsed_data,
        UNNEST(
            CAST(json_extract(uint_items, '$.items') AS ARRAY(JSON))
        ) AS t(item)
)


, int_items_parsed AS (
    SELECT 
        tx_hash,
        index,
        json_extract_scalar(CAST(item AS VARCHAR), '$.key') AS key_name,
        json_extract_scalar(CAST(item AS VARCHAR), '$.value') AS value
    FROM 
        parsed_data,
        UNNEST(
            CAST(json_extract(int_items, '$.items') AS ARRAY(JSON))
        ) AS t(item)
)

, bytes32_items_parsed AS (
    SELECT 
        tx_hash,
        index,
        json_extract_scalar(CAST(item AS VARCHAR), '$.key') AS key_name,
        json_extract_scalar(CAST(item AS VARCHAR), '$.value') AS value
    FROM 
        parsed_data,
        UNNEST(
            CAST(json_extract(bytes32_items, '$.items') AS ARRAY(JSON))
        ) AS t(item)
)

, combined AS (
    SELECT *
    FROM address_items_parsed
    UNION ALL
    SELECT *
    FROM uint_items_parsed
    UNION ALL 
    SELECT *
    FROM int_items_parsed
    UNION ALL
    SELECT *
    FROM bytes32_items_parsed
)

, evt_data_parsed AS (
    SELECT
        tx_hash,
        index,

        MAX(CASE WHEN key_name = 'market' THEN value END) AS market,
        MAX(CASE WHEN key_name = 'receiver' THEN value END) AS receiver,
        MAX(CASE WHEN key_name = 'tokenIn' THEN value END) AS token_in,
        MAX(CASE WHEN key_name = 'tokenOut' THEN value END) AS token_out,
        MAX(CASE WHEN key_name = 'tokenInPrice' THEN value END) AS token_in_price,
        MAX(CASE WHEN key_name = 'tokenOutPrice' THEN value END) AS token_out_price,
        MAX(CASE WHEN key_name = 'amountIn' THEN value END) AS amount_in,
        MAX(CASE WHEN key_name = 'amountInAfterFees' THEN value END) AS amount_in_after_fees,
        MAX(CASE WHEN key_name = 'amountOut' THEN value END) AS amount_out,
        MAX(CASE WHEN key_name = 'priceImpactUsd' THEN value END) AS price_impact_usd,
        MAX(CASE WHEN key_name = 'priceImpactAmount' THEN value END) AS price_impact_amount,
        MAX(CASE WHEN key_name = 'tokenInPriceImpactAmount' THEN value END) AS token_in_price_impact_amount,
        MAX(CASE WHEN key_name = 'orderKey' THEN value END) AS order_key
        
    FROM
        combined
    GROUP BY tx_hash, index
)

, event_data AS (
    SELECT 
        blockchain,
        block_time,
        block_number,
        ED.tx_hash,
        ED.index,
        contract_address,
        event_name,
        msg_sender,

        from_hex(market) AS market,
        from_hex(receiver) AS receiver,
        from_hex(token_in) AS token_in,
        from_hex(token_out) AS token_out,
        TRY_CAST(token_in_price AS DOUBLE) AS token_in_price,
        TRY_CAST(token_out_price AS DOUBLE) AS token_out_price,
        TRY_CAST(amount_in AS DOUBLE) AS amount_in,
        TRY_CAST(amount_in_after_fees AS DOUBLE) AS amount_in_after_fees,
        TRY_CAST(amount_out AS DOUBLE) AS amount_out,
        TRY_CAST(price_impact_usd AS DOUBLE) AS price_impact_usd,
        TRY_CAST(price_impact_amount AS DOUBLE) AS price_impact_amount,
        TRY_CAST(token_in_price_impact_amount AS DOUBLE) AS token_in_price_impact_amount,
        from_hex(order_key) AS order_key
        
    FROM evt_data AS ED
    LEFT JOIN evt_data_parsed AS EDP
        ON ED.tx_hash = EDP.tx_hash
        AND ED.index = EDP.index
)

, full_data AS (
    SELECT 
        ED.blockchain,
        block_time,
        DATE(block_time) AS block_date,
        block_number,
        tx_hash,
        index,
        ED.contract_address,
        event_name,
        msg_sender,

        ED.market,
        receiver,
        token_in,
        token_out,
        token_in_price / POWER(10, 30 - ERC20_in.decimals) AS token_in_price,
        token_out_price / POWER(10, 30 - ERC20_out.decimals) AS token_out_price,
        amount_in / POWER(10, ERC20_in.decimals) AS amount_in,
        amount_in_after_fees / POWER(10, ERC20_in.decimals) AS amount_in_after_fees,
        amount_out / POWER(10, ERC20_out.decimals) AS amount_out,
        price_impact_usd / POWER(10, 30) AS price_impact_usd,
        CASE
            WHEN price_impact_amount > 0 THEN price_impact_amount / POWER(10, ERC20_out.decimals)
            WHEN price_impact_amount < 0 THEN price_impact_amount / POWER(10, ERC20_in.decimals)
            ELSE price_impact_amount
        END AS price_impact_amount,
        token_in_price_impact_amount / POWER(10, ERC20_in.decimals) AS token_in_price_impact_amount,
        order_key
    FROM event_data AS ED
    LEFT JOIN {{ ref('gmx_v2_arbitrum_erc20') }} AS ERC20_in
        ON ED.token_in = ERC20_in.contract_address
    LEFT JOIN {{ ref('gmx_v2_arbitrum_erc20') }} AS ERC20_out
        ON ED.token_out = ERC20_out.contract_address
)

--can be removed once decoded tables are fully denormalized
{{
    add_tx_columns(
        model_cte = 'full_data'
        , blockchain = blockchain_name
        , columns = ['from', 'to']
    )
}}