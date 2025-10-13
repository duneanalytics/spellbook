{{config(
    schema = 'tokens_tron',
    alias = 'base_transfers',
    partition_by = ['block_month'],
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')],
    unique_key = ['block_date','unique_key'],
)
}}

-- TRC20 (ERC20) transfers
SELECT
    {{dbt_utils.generate_surrogate_key(['t.evt_block_number', 'tx.index', 't.evt_index'])}} as unique_key
    , 'tron' as blockchain
    , cast(date_trunc('month', t.evt_block_time) as date) AS block_month
    , cast(date_trunc('day', t.evt_block_time) as date) AS block_date
    , t.evt_block_time AS block_time
    , t.evt_block_number AS block_number
    , t.evt_tx_hash AS tx_hash
    , SUBSTR( cast(t.evt_tx_hash as varchar), 3) as tx_hash_varchar
    , t.evt_index
    , CAST(NULL AS ARRAY<BIGINT>) AS trace_address
    , t.contract_address
    , to_tron_address(t.contract_address) as contract_address_varchar
    , 'trc20' AS token_standard
    , t."from"
    , to_tron_address(t."from") as from_varchar
    , t.to
    , to_tron_address(t.to) as to_varchar
    , tx."index" AS tx_index
    , tx."from" as tx_from
    , to_tron_address(tx."from") as tx_from_varchar
    , tx.to as tx_to
    , to_tron_address(tx.to) as tx_to_varchar
    , t.value AS amount_raw
FROM {{ source('erc20_tron','evt_Transfer') }} t
INNER JOIN {{ source('tron','transactions') }} tx 
    ON tx.block_date = t.evt_block_date
    AND tx.block_time = t.evt_block_time 
    AND tx.block_number = t.evt_block_number
    AND tx.hash = t.evt_tx_hash
    {% if is_incremental() %}
    AND {{incremental_predicate('tx.block_time')}}
    {% endif %}
{% if is_incremental() %}
WHERE {{incremental_predicate('t.evt_block_time')}}
{% endif %}

UNION ALL

-- Native TRX transfers
SELECT
    {{dbt_utils.generate_surrogate_key(['tx.block_number', 'tx.index'])}} as unique_key
    , 'tron' as blockchain
    , cast(date_trunc('month', tx.block_time) as date) AS block_month
    , cast(date_trunc('day', tx.block_time) as date) AS block_date
    , tx.block_time AS block_time
    , tx.block_number AS block_number
    , tx.hash AS tx_hash
    , SUBSTR( cast(tx.hash as varchar), 3) as tx_hash_varchar
    , CAST(NULL AS BIGINT) AS evt_index
    , CAST(NULL AS ARRAY<BIGINT>) AS trace_address
    , (SELECT token_address FROM {{source('dune','blockchains')}} WHERE name = 'tron') AS contract_address
    , to_tron_address((SELECT token_address FROM {{source('dune','blockchains')}} WHERE name = 'tron')) as contract_address_varchar
    , 'native' AS token_standard
    , tx."from"
    , to_tron_address(tx."from") as from_varchar
    , tx.to
    , to_tron_address(tx.to) as to_varchar
    , tx."index" AS tx_index
    , tx."from" as tx_from
    , to_tron_address(tx."from") as tx_from_varchar
    , tx.to as tx_to
    , to_tron_address(tx.to) as tx_to_varchar
    , tx.value AS amount_raw
FROM {{ source('tron','transactions') }} tx
WHERE tx.success = true
    AND tx.value > UINT256 '0'
    {% if is_incremental() %}
    AND {{incremental_predicate('tx.block_time')}}
    {% endif %}
