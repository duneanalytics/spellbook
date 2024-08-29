{{ config(
    schema = 'rollup_economics_arbitrum'
    , alias = 'l1_data_fees'
    , materialized = 'incremental'
    , file_format = 'delta'
    , incremental_strategy = 'merge'
    , unique_key = ['name', 'tx_hash']
    , incremental_predicates = [incremental_predicate('DBT_INTERNAL_DEST.block_time')]
)}}

SELECT
    'arbitrum' AS name
    , cast(date_trunc('month', t.block_time) AS date) AS block_month
    , cast(date_trunc('day', t.block_time) AS date) AS block_date
    , t.block_time
    , t.block_number
    , t.hash AS tx_hash
    , t.index AS tx_index
    , t.gas_price
    , t.gas_used
    , (t.gas_price / 1e18) * t.gas_used AS data_fee_native
    , {{ evm_get_calldata_gas_from_data('t.data') }} AS calldata_gas_used
    , (length(t.data)) AS data_length
FROM
    (
    SELECT
        evt_tx_hash as tx_hash,
        evt_block_time as block_time,
        evt_block_number as block_number
    FROM {{ source('arbitrum_ethereum', 'SequencerInbox_evt_SequencerBatchDeliveredFromOrigin') }} o
    WHERE evt_block_time >= timestamp '2022-01-01'
    {% if is_incremental() %}
    AND {{incremental_predicate('evt_block_time')}}
    {% endif %}

    UNION ALL

    SELECT
        call_tx_hash as tx_hash,
        call_block_time as block_time,
        call_block_number as block_number
    FROM {{ source('arbitrum_ethereum','SequencerInbox_call_addSequencerL2BatchFromOrigin') }} o
    WHERE call_success = true
    AND call_tx_hash NOT IN
    (
    SELECT evt_tx_hash FROM {{ source('arbitrum_ethereum', 'SequencerInbox_evt_SequencerBatchDeliveredFromOrigin') }} o
    WHERE evt_block_time >= timestamp '2022-01-01'
    )
    {% if is_incremental() %}
    AND {{incremental_predicate('call_block_time')}}
    {% endif %}

    UNION ALL

    SELECT
        call_tx_hash as tx_hash,
        call_block_time as block_time,
        call_block_number as block_number
    FROM {{ source('arbitrum_ethereum','SequencerInbox_call_addSequencerL2Batch') }} o
    WHERE call_success = true
    AND call_tx_hash NOT IN
    (
    SELECT evt_tx_hash FROM {{ source('arbitrum_ethereum', 'SequencerInbox_evt_SequencerBatchDeliveredFromOrigin') }} o
    WHERE evt_block_time >= timestamp '2022-01-01'
    )
    {% if is_incremental() %}
    AND {{incremental_predicate('call_block_time')}}
    {% endif %}

    UNION ALL

    SELECT
        call_tx_hash as tx_hash,
        call_block_time as block_time,
        call_block_number as block_number
    FROM {{ source('arbitrum_ethereum','SequencerInbox_call_addSequencerL2BatchFromOriginWithGasRefunder') }} o
    WHERE call_success = true
    AND call_tx_hash NOT IN
    (
    SELECT evt_tx_hash FROM {{ source('arbitrum_ethereum', 'SequencerInbox_evt_SequencerBatchDeliveredFromOrigin') }} o
    WHERE evt_block_time >= timestamp '2022-01-01'
    )
    {% if is_incremental() %}
    AND {{incremental_predicate('call_block_time')}}
    {% endif %}

    UNION ALL

    SELECT
        hash as tx_hash,
        block_time,
        block_number
    FROM {{ source('ethereum','transactions') }}
    WHERE "from" = 0xC1b634853Cb333D3aD8663715b08f41A3Aec47cc
    AND to = 0x1c479675ad559DC151F6Ec7ed3FbF8ceE79582B6
    AND bytearray_substring(data, 1, 4) = 0x3e5aa082 --addSequencerL2BatchFromBlobs
    AND block_number >= 19433943 --when arbitrum started submitting blobs
    {% if is_incremental() %}
    AND {{incremental_predicate('block_time')}}
    {% endif %}
) b
INNER JOIN {{ source('ethereum','transactions') }} t
    ON b.tx_hash = t.hash
    AND b.block_number = t.block_number
    AND t.success = true
    AND t.block_time >= timestamp '2022-01-01'
    {% if is_incremental() %}
    AND {{incremental_predicate('t.block_time')}}
    {% endif %}