{{config(
    schema = 'tokens_gnosis',
    alias = 'base_non_standard_transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_date','unique_key'],
)
}}

WITH 

gas_fee as (
    SELECT 
        'gas_fee' as transfer_type
        , cast(date_trunc('month', block_time) as date) AS block_month
        , cast(date_trunc('day', block_time) as date) AS block_date
        , block_time AS block_time
        , block_number AS block_number
        , hash AS tx_hash
        , NULL AS evt_index
        , CAST(NULL AS ARRAY<BIGINT>) AS trace_address
        , CAST(NULL AS varbinary) AS contract_address
        , 'native' AS token_standard
        , "from"
        , CAST(NULL AS varbinary) AS to
        , gas_used * gas_price AS amount_raw
    FROM 
    {{ source('gnosis', 'transactions') }}
    WHERE 
       gas_price != UINT256 '0'
    {% if is_incremental() %}
        AND {{incremental_predicate('block_time')}}
    {% endif %}
),

gas_fee_collection as (
    SELECT 
        'gas_fee_collection' as transfer_type
        , cast(date_trunc('month', t1.block_time) as date) AS block_month
        , cast(date_trunc('day', t1.block_time) as date) AS block_date
        , t1.block_time AS block_time
        , t1.block_number AS block_number
        , t1.hash AS tx_hash
        , NULL AS evt_index
        , CAST(NULL AS ARRAY<BIGINT>) AS trace_address
        , CAST(NULL AS varbinary) AS contract_address
        , 'native' AS token_standard
        , CAST(NULL AS varbinary) AS "from"
        , 0x6BBe78ee9e474842Dbd4AB4987b3CeFE88426A92 AS to -- fee collector
        , t1.gas_used * COALESCE(t2.base_fee_per_gas, CAST(0 AS UINT256)) AS amount_raw
    FROM 
        {{ source('gnosis', 'transactions') }} t1
    INNER JOIN
        {{ source('gnosis', 'blocks') }} t2
        ON
        t2.number = t1.block_number
    WHERE 
        t1.gas_price != UINT256 '0'
    {% if is_incremental() %}
        AND {{incremental_predicate('t1.block_time')}}
        AND {{incremental_predicate('t2.time')}}
    {% endif %}
),

gas_fee_rewards as (
    SELECT 
        'gas_fee_rewards' as transfer_type
        , cast(date_trunc('month', t1.block_time) as date) AS block_month
        , cast(date_trunc('day', t1.block_time) as date) AS block_date
        , t1.block_time AS block_time
        , t1.block_number AS block_number
        , t1.hash AS tx_hash
        , NULL AS evt_index
        , CAST(NULL AS ARRAY<BIGINT>) AS trace_address
        , CAST(NULL AS varbinary) AS contract_address
        , 'native' AS token_standard
        , CAST(NULL AS varbinary) AS "from"
        , t2.miner AS to 
        ,t1.gas_used * ( t1.gas_price - COALESCE(t2.base_fee_per_gas,CAST(0 AS UINT256)) ) AS amount_raw
    FROM 
        {{ source('gnosis', 'transactions') }} t1
    INNER JOIN
        {{ source('gnosis', 'blocks') }} t2
        ON
        t2.number = t1.block_number
    WHERE   
       t1.gas_price != UINT256 '0'
     {% if is_incremental() %}
        AND {{incremental_predicate('t1.block_time')}}
        AND {{incremental_predicate('t2.time')}}
    {% endif %}
),

block_reward AS (
    SELECT 
        'block_reward' as transfer_type
        , cast(date_trunc('month', evt_block_time) as date) AS block_month
        , cast(date_trunc('day', evt_block_time) as date) AS block_date
        , evt_block_time AS block_time
        , evt_block_number AS block_number
        , evt_tx_hash AS tx_hash
        , evt_index
        , CAST(NULL AS ARRAY<BIGINT>) AS trace_address
        , CAST(NULL AS varbinary) AS contract_address
        , 'native' AS token_standard
        , CAST(NULL AS varbinary) AS "from"
        , receiver AS to 
        , amount AS amount_raw
    FROM 
        {{ source('xdai_gnosis', 'RewardByBlock_evt_AddedReceiver') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% endif %}

    UNION ALL

    SELECT 
        'block_reward' as transfer_type
        , cast(date_trunc('month', evt_block_time) as date) AS block_month
        , cast(date_trunc('day', evt_block_time) as date) AS block_date
        , evt_block_time AS block_time
        , evt_block_number AS block_number
        , evt_tx_hash AS tx_hash
        , evt_index
        , CAST(NULL AS ARRAY<BIGINT>) AS trace_address
        , CAST(NULL AS varbinary)  AS contract_address
        , 'native' AS token_standard
        , CAST(NULL AS varbinary) AS "from"
        , receiver AS to 
        , amount AS amount_raw
    FROM 
        {{ source('xdai_gnosis', 'BlockRewardAuRa_evt_AddedReceiver') }}
    {% if is_incremental() %}
    WHERE {{incremental_predicate('evt_block_time')}}
    {% endif %}
),

non_standard_transfers AS (
    SELECT * FROM gas_fee
    UNION ALL
    SELECT * FROM gas_fee_collection
    UNION ALL
    SELECT * FROM gas_fee_rewards
    UNION ALL
    SELECT * FROM block_reward
)


SELECT 
    -- We have to create this unique key because evt_index and trace_address can be null
    {{dbt_utils.generate_surrogate_key(['t.block_number', 'tx.index', 't.transfer_type', "array_join(t.trace_address, ',')"])}} as unique_key
    , t.transfer_type
    , 'gnosis' as blockchain
    , t.block_month
    , t.block_date
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
FROM non_standard_transfers t
INNER JOIN {{ source('gnosis', 'transactions') }} tx ON
    cast(date_trunc('day', tx.block_time) as date) = t.block_date 
    AND tx.block_number = t.block_number
    AND tx.hash = t.tx_hash
    {% if is_incremental() %}
    AND {{incremental_predicate('tx.block_time')}}
    {% endif %}
WHERE  
    t.amount_raw > UINT256 '0'