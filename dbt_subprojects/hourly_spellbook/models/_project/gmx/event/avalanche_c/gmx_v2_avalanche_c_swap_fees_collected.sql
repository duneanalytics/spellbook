{{
  config(
    schema = 'gmx_v2_avalanche_c',
    alias = 'swap_fees_collected',
    materialized = 'incremental',
    unique_key = ['tx_hash', 'index'],
    incremental_strategy = 'merge'
    )
}}

{%- set event_name = 'SwapFeesCollected' -%}
{%- set blockchain_name = 'avalanche_c' -%}

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
    FROM {{ source('gmx_v2_avalanche_c','EventEmitter_evt_EventLog1')}}
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
    FROM {{ source('gmx_v2_avalanche_c','EventEmitter_evt_EventLog2')}}
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
    FROM bytes32_items_parsed
)

, evt_data_parsed AS (
    SELECT
        tx_hash,
        index,

        MAX(CASE WHEN key_name = 'uiFeeReceiver' THEN value END) AS ui_fee_receiver,
        MAX(CASE WHEN key_name = 'market' THEN value END) AS market,
        MAX(CASE WHEN key_name = 'token' THEN value END) AS token,
        
        MAX(CASE WHEN key_name = 'tokenPrice' THEN value END) AS token_price,
        MAX(CASE WHEN key_name = 'feeReceiverAmount' THEN value END) AS fee_receiver_amount,
        MAX(CASE WHEN key_name = 'feeAmountForPool' THEN value END) AS fee_amount_for_pool,
        MAX(CASE WHEN key_name = 'amountAfterFees' THEN value END) AS amount_after_fees,
        MAX(CASE WHEN key_name = 'uiFeeReceiverFactor' THEN value END) AS ui_fee_receiver_factor,
        MAX(CASE WHEN key_name = 'uiFeeAmount' THEN value END) AS ui_fee_amount,

        MAX(CASE WHEN key_name = 'tradeKey' THEN value END) AS trade_key,
        MAX(CASE WHEN key_name = 'swapFeeType' THEN value END) AS swap_fee_type
        
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

        from_hex(ui_fee_receiver) AS ui_fee_receiver,
        from_hex(market) AS market,
        from_hex(token) AS token,

        TRY_CAST(token_price AS DOUBLE) token_price,
        TRY_CAST(fee_receiver_amount AS DOUBLE) fee_receiver_amount,
        TRY_CAST(fee_amount_for_pool AS DOUBLE) fee_amount_for_pool,
        TRY_CAST(amount_after_fees AS DOUBLE) amount_after_fees,
        TRY_CAST(ui_fee_receiver_factor AS DOUBLE) ui_fee_receiver_factor,
        TRY_CAST(ui_fee_amount AS DOUBLE) ui_fee_amount,

        from_hex(trade_key) AS trade_key,
        from_hex(swap_fee_type) AS swap_fee_type
        
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

        ui_fee_receiver,
        market,
        ED.token,

        token_price / POWER(10, 30 - ERC20.decimals) AS token_price,
        fee_receiver_amount / POWER(10, ERC20.decimals) AS fee_receiver_amount,
        fee_amount_for_pool / POWER(10, ERC20.decimals) AS fee_amount_for_pool,
        amount_after_fees / POWER(10, ERC20.decimals) AS amount_after_fees,
        ui_fee_receiver_factor / POWER(10, 30) AS ui_fee_receiver_factor,
        ui_fee_amount / POWER(10, ERC20.decimals) AS ui_fee_amount,

        trade_key,
        swap_fee_type,
        CASE 
            WHEN swap_fee_type = 0x39226eb4fed85317aa310fa53f734c7af59274c49325ab568f9c4592250e8cc5 THEN 'deposit'
            WHEN swap_fee_type = 0xda1ac8fcb4f900f8ab7c364d553e5b6b8bdc58f74160df840be80995056f3838 THEN 'withdrawal'
            WHEN swap_fee_type = 0x7ad0b6f464d338ea140ff9ef891b4a69cf89f107060a105c31bb985d9e532214 THEN 'swap'
        END AS action_type
        
    FROM event_data AS ED
    LEFT JOIN {{ ref('gmx_v2_avalanche_c_erc20') }} AS ERC20
        ON ED.token = ERC20.contract_address
)

--can be removed once decoded tables are fully denormalized
{{
    add_tx_columns(
        model_cte = 'full_data'
        , blockchain = blockchain_name
        , columns = ['from', 'to']
    )
}}