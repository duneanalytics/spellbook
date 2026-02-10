{{ config(
    schema = 'ethereum',
    alias = 'blobs_submissions',
    materialized = 'incremental',
    file_format = 'delta',
    incremental_strategy = 'merge',
    unique_key = ['block_number','tx_index']
)}}

with blob_transactions as (
SELECT *
FROM {{ source('ethereum','transactions') }}
WHERE type = '3'
AND block_number >= 19426587 -- dencun upgrade
{% if is_incremental() %}
AND {{ incremental_predicate('block_time')}}
{% endif %}
)
, blocks_enriched as (
    SELECT
         CASE
            WHEN number >= 24179383 THEN 14*128*1024 -- BPO2 Fusaka (14 blobs)
            WHEN number >= 23975778 THEN 10*128*1024 -- BPO1 Fusaka (10 blobs)
            WHEN number >= 22431084 THEN 6*128*1024 -- Pectra & Fusaka (6 blobs)
            ELSE 3*128*1024 -- Dencun (3 blobs of 128kib)
        END AS target_blob_gas,
        CASE
            WHEN number >= 24179383 THEN 11684671 -- BPO2 Fusaka
            WHEN number >= 23975778 THEN 8346193 -- BPO1 Fusaka
            WHEN number >= 22431084 THEN 5007716 -- Pectra & Fusaka
            ELSE 3338477 -- Dencun
        END AS blob_base_fee_update_fraction,
        b.*
    FROM {{ source('ethereum', 'blocks') }} b
    WHERE number >= 19426587 -- since Dencun
    {% if is_incremental() %}
    AND {{ incremental_predicate('b.time') }}
    {% endif %}
),
blocks_with_blob_base_fee AS (
    SELECT
        *,
        CAST(
            FLOOR(
                EXP(
                    CAST(excess_blob_gas AS DOUBLE) / CAST(blob_base_fee_update_fraction AS DOUBLE)
                )
            )
        AS BIGINT) AS blob_base_fee
    FROM blocks_enriched
)
SELECT
     t.block_number
    , t.block_time
    , t.block_date
    , beacon.slot as beacon_slot_number
    , beacon.epoch as beacon_epoch
    , t.value as tx_value
    , t.hash as tx_hash
    , t."from" as blob_submitter
    , t.to as blob_receiver
    , COALESCE(l.entity, ls.entity) as blob_submitter_label
    , l.proposer as blob_proposer_label
    , t.index as tx_index
    , t.success as tx_success
    , t.data as tx_data
    , t.type as tx_type
    , t.blob_versioned_hashes
    , sequence(
        (sum(CARDINALITY(t.blob_versioned_hashes)) over (partition by t.block_number order by index asc)) - CARDINALITY(t.blob_versioned_hashes)
        ,(sum(CARDINALITY(t.blob_versioned_hashes)) over (partition by t.block_number order by index asc)) - 1
        ,1
    ) as blob_indexes
    , CARDINALITY(t.blob_versioned_hashes) AS blob_count
    , CARDINALITY(t.blob_versioned_hashes) * pow(2,17) as blob_gas_used -- within this tx
    , block.blob_base_fee
    , t.max_fee_per_blob_gas
    , coalesce(("LEFT"(from_utf8(t.data), 5)='data:'), false) as is_blobscription
FROM blob_transactions t
INNER JOIN blocks_with_blob_base_fee block
    ON t.block_number = block.number
    AND block.number >= 19426587    -- dencun upgrade
    {% if is_incremental() %}
    and {{ incremental_predicate('t.block_time') }}
    {% endif %}
INNER JOIN {{ source('beacon', 'blocks') }} beacon
    ON beacon.parent_root = block.parent_beacon_block_root
    AND beacon.slot >= 8626176 -- dencun upgrade
    {% if is_incremental() %}
    and {{ incremental_predicate('beacon.time') }}
    {% endif %}
LEFT JOIN {{ref('blobs_submitters')}} l
    ON t."from" = l.address
LEFT JOIN {{ref('blobs_based_submitters')}} ls
    ON t.block_number = ls.block_number
    AND t.hash = ls.tx_hash
