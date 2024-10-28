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

-- this is only including TRC20 (ERC20) transfers
SELECT
    {{dbt_utils.generate_surrogate_key(['t.block_number', 'tx.index', 't.evt_index'])}} as unique_key
    , 'tron' as blockchain
    , cast(date_trunc('month', t.evt_block_time) as date) AS block_month
    , cast(date_trunc('day', t.evt_block_time) as date) AS block_date
    , t.evt_block_time AS block_time
    , t.evt_block_number AS block_number
    , t.evt_tx_hash AS tx_hash
    , t.evt_index
    , CAST(NULL AS ARRAY<BIGINT>) AS trace_address
    , t.contract_address
    , 'trc20' AS token_standard
    , t."from"
    , t.to
    , tx."index" AS tx_index
    , tx."from" as tx_from
    , tx.to as tx_to
    , t.contract_address
    , t.value AS amount_raw
FROM {{ source('erc20_tron','evt_transfer' }} t
INNER JOIN {{ source('tron','transactions') }} tx ON
    tx.block_date = t.block_date -- partition column in raw base tables (transactions)
    AND tx.block_number = t.block_number
    AND tx.hash = t.tx_hash
    {% if is_incremental() %}
    AND {{incremental_predicate('tx.block_time')}}
    {% endif %}
{% if is_incremental() %}
WHERE {{incremental_predicate('t.evt_block_time')}}
{% endif %}
