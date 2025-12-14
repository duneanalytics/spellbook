{{ config(
    schema = 'toroperp_sei',
    alias = 'deposits',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_date', 'tx_hash', 'evt_index'],
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    post_hook='{{ expose_spells(blockchains = \'["sei"]\',
                                spell_type = "project",
                                spell_name = "toroperp",
                                contributors = \'["toroperp"]\') }}'
    )
}}

/*
    Toroperp broker deposits on Orderly Network (SEI)

    Filters deposits to the Orderly Vault contract by toroperp's broker hash.
    The brokerHash is extracted from the deposit transaction input data.

    Vault Contract: 0x816f722424B49Cf1275cc86DA9840Fbd5a6167e9
    Broker Hash: 0x18749138d2f6349916b7fbdc3a498a292169a4e698b2f3d66c8bb3f4249098ce

    Deposit function signature: deposit((bytes32,bytes32,bytes32,uint128))
    Input data structure:
    - bytes32[0]: accountId
    - bytes32[1]: brokerHash
    - bytes32[2]: tokenHash
    - uint128: tokenAmount
*/

{% set project_start_date = '2024-05-27' %}
{% set vault_contract = '0x816f722424B49Cf1275cc86DA9840Fbd5a6167e9' %}
{% set toroperp_broker_hash = '0x18749138d2f6349916b7fbdc3a498a292169a4e698b2f3d66c8bb3f4249098ce' %}

-- AccountDepositTo event signature: keccak256("AccountDepositTo(bytes32,address,uint64,bytes32,uint128)")
{% set account_deposit_to_topic = '0x11f843b2ed43e9b4b568b4dff0c777a6c5ca538b4115a6149f28bce4bea90148' %}

WITH deposit_events AS (
    SELECT
        l.block_time
        ,CAST(l.block_time AS DATE) AS block_date
        ,l.block_number
        ,l.tx_hash
        ,l.index AS evt_index
        ,l.contract_address AS vault_contract
        -- Decode indexed parameters from topics
        ,l.topic1 AS account_id  -- bytes32 indexed accountId
        ,CAST(varbinary_substring(l.topic2, 13, 20) AS varbinary) AS user_address  -- address indexed userAddress (last 20 bytes)
        ,varbinary_to_uint256(l.topic3) AS deposit_nonce  -- uint64 indexed depositNonce
        -- Decode non-indexed parameters from data (ABI encoding: 32 bytes per value)
        ,varbinary_substring(l.data, 1, 32) AS token_hash  -- bytes32 tokenHash
        ,varbinary_to_uint256(varbinary_substring(l.data, 33, 32)) AS token_amount_raw  -- uint128 tokenAmount (32 bytes, left-padded)
        ,t."from" AS tx_from
        ,t.to AS tx_to
        ,t.data AS tx_input_data
    FROM
        {{ source('sei', 'logs') }} l
    INNER JOIN
        {{ source('sei', 'transactions') }} t
        ON l.tx_hash = t.hash
        AND l.block_number = t.block_number
    WHERE
        l.contract_address = {{ vault_contract }}
        AND l.topic0 = {{ account_deposit_to_topic }}
        {% if is_incremental() %}
        AND {{ incremental_predicate('l.block_time') }}
        {% else %}
        AND l.block_time >= TIMESTAMP '{{ project_start_date }}'
        {% endif %}
),

-- Extract brokerHash from transaction input data
-- deposit((bytes32,bytes32,bytes32,uint128)) has selector 0x322dda6d
-- Data layout after 4-byte selector:
-- offset 0-32: accountId (bytes32)
-- offset 32-64: brokerHash (bytes32)
-- offset 64-96: tokenHash (bytes32)
-- offset 96-112: tokenAmount (uint128)
deposits_with_broker AS (
    SELECT
        d.*
        ,varbinary_substring(d.tx_input_data, 37, 32) AS broker_hash  -- bytes 36-68 (after 4-byte selector + 32-byte accountId)
    FROM
        deposit_events d
    WHERE
        -- Filter only deposit function calls (selector: 0x322dda6d)
        varbinary_substring(d.tx_input_data, 1, 4) = 0x322dda6d
)

SELECT
    'sei' AS blockchain
    ,'toroperp' AS project
    ,'1' AS version
    ,block_time
    ,block_date
    ,block_number
    ,tx_hash
    ,evt_index
    ,vault_contract
    ,account_id
    ,CAST(user_address AS varbinary) AS user_address
    ,deposit_nonce
    ,token_hash
    ,token_amount_raw
    ,tx_from
    ,tx_to
    ,broker_hash
FROM
    deposits_with_broker
WHERE
    broker_hash = {{ toroperp_broker_hash }}
